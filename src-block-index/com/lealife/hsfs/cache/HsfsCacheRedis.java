package com.lealife.hsfs.cache;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;

/**
 * 数据缓存机制, 包括:
 * 	put file cache
 * 	缓存从block get后的数据 
 * 
 * put file:
 * 表名, putFileCache : {fileId1 => byte[], fileId2 => byte[]}. 一行记录
 * putFileSize: 12 字节大小. 一行记录
 * 
 * block get:
 * 表名, hdfsCache:
 * 	{hdfsPath => {fileId1 => file1, fileId2 => file2} }
 * 
 * 表名, hdfsCacheLRU 最近使用, map, 存储最近使用的hdfsPath(只有一行记录)
 * {hadfsPath => timestamp}
 * 
 * @author life
 *
 */
public class HsfsCacheRedis implements HsfsCacheInterface {
	static Logger logger = Logger.getLogger(HsfsCacheRedis.class);
    
	static Jedis jedis = new Jedis("localhost"); 
    
    /**
     * 最多缓存块数
     * 一个block 64M, 那么就有640M
     */
	static int maxCahceBlockSize = 1; // 10 * Block
        
	static String putFileCahceTable = "putFileCache";
	static String putFileCahceSizeTable = "putFileCacheSize";
    
	static String hdfsCahceTable = "hdfsCache";
	static String hdfsCacheLRUTable = "hdfsCacheLRU";
    
    /* (non-Javadoc)
	 * @see com.lealife.hsfs.HsfsCacheInterface#addPutFile(java.lang.String, byte[])
	 */
	@Override
	public void addPutFile(String fileId, byte[] content) {
        jedis.hset(putFileCahceTable.getBytes(), fileId.getBytes(), content);
        int preSize = getPutFilesSize();
        jedis.set(putFileCahceSizeTable, (preSize + content.length) + "");
	}
    
    /* (non-Javadoc)
	 * @see com.lealife.hsfs.HsfsCacheInterface#getPutFilesSize()
	 */
	@Override
	public int getPutFilesSize() {
        String size = jedis.get(putFileCahceSizeTable);
        if(size == null) {
        	return 0;
        }
		return Integer.parseInt(size);
	}
    
    /**
     * 得到某个file的大小
     * @param fileId
     * @return
     */
	private int getPutFileSize(String fileId) {
        byte[] file = jedis.hget(putFileCahceTable.getBytes(), fileId.getBytes());
        if(file == null) {
        	return 0;
        }
        int size = file.length;
        file = null;
        return size;
	}
    
    /* (non-Javadoc)
	 * @see com.lealife.hsfs.HsfsCacheInterface#clearPutFiles()
	 */
	@Override
	public void clearPutFiles() {
        jedis.set(putFileCahceSizeTable, "0");
        jedis.del(putFileCahceTable);
	}
    
    /* (non-Javadoc)
	 * @see com.lealife.hsfs.HsfsCacheInterface#clearPutFiles(java.util.Set)
	 */
	@Override
	public void clearPutFiles(Set<String> fileIds) {
        // 要减少的总大小
        int size = 0;
        int eachFileSize;
        for(String fileId : fileIds) {
            eachFileSize = getPutFileSize(fileId);
            if(eachFileSize > 0) {
                size += eachFileSize;
                // 删除之
                jedis.hdel(putFileCahceTable, fileId);
            } else {
            	logger.info("有文件为空: " + fileId);
            }
        }
        
        // 设置大小
        int preSize = getPutFilesSize();
        int endSize = preSize - size;
        if(endSize < 0) {
            logger.info("size < 0");
        	endSize = 0;
        } else if(endSize > 0) {
            logger.info("size > 0");
        }
        logger.info("preSize: " + preSize + "; endsize: " + endSize);
        
        jedis.set(putFileCahceSizeTable, endSize + "");
	}
    
    /* (non-Javadoc)
	 * @see com.lealife.hsfs.HsfsCacheInterface#getPutFiles()
	 */
	@Override
	public Map<String, byte[]> getPutFiles() {
		Map<byte[], byte[]> putFiles = jedis.hgetAll(putFileCahceTable.getBytes());
        Map<String, byte[]> files = new HashMap<String, byte[]>();
        for(Entry<byte[], byte[]> each: putFiles.entrySet()) {
            files.put(new String(each.getKey()), each.getValue());
        }
        return files;
	}
    
    /**
     * 得到put file
     * @param fileId
     * @return
     */
	private byte[] getPutFile(String fileId) {
		return jedis.hget(putFileCahceTable.getBytes(), fileId.getBytes());
	}
    
	//------------------
 
    /* (non-Javadoc)
	 * @see com.lealife.hsfs.HsfsCacheInterface#getCache(java.lang.String, java.lang.String)
	 */
	@Override
	public byte[] getCache(String hdfsPath, String fileId) {
        // 先从putFileCache中得到
        byte[] putFile = getPutFile(fileId);
		if(putFile != null && putFile.length > 0) {
            logger.info("putFile cache 中获取");
			return putFile;
		}
        
        if(updateLru(hdfsPath)) {
            return jedis.hget(genKey(hdfsCahceTable, hdfsPath).getBytes(), fileId.getBytes());
        }
        return null;
	}
    
    /* (non-Javadoc)
	 * @see com.lealife.hsfs.HsfsCacheInterface#setCache(java.lang.String, java.util.Map)
	 */
	@Override
	public boolean setCache(String hdfsPath, Map<String, byte[]> files) {
        // 检查当前缓存大小, 大了就要踢了
        Map<String, String> lruMap = jedis.hgetAll(hdfsCacheLRUTable);
        // 如果超过, 按时间排序, 最小的踢掉
        logger.info("set cahce 当前cache大小: " + lruMap.size());
        if(lruMap.size() >= maxCahceBlockSize) {
            String minKey = null;
            String minTime = "99999999999999999999";
            for(Map.Entry<String, String> eachEntry : lruMap.entrySet()) {
                if(minTime.compareTo(eachEntry.getValue()) > 0) {
                	minTime = eachEntry.getValue();
                    minKey = eachEntry.getKey();
                }
            }
            logger.info("踢cache" + minKey);
            // OK, 最后得到当前最久没有访问的hdfsPath, 即minKey
            // 将缓存与lru都删除掉
            jedis.hdel(hdfsCacheLRUTable, minKey);
            jedis.del(genKey(hdfsCahceTable, minKey));
        }
        
        // 添加缓存吧
        byte[] key = genKey(hdfsCahceTable, hdfsPath).getBytes();
        for(Map.Entry<String, byte[]> each : files.entrySet()) {
            jedis.hset(key, each.getKey().getBytes(), each.getValue());
        }
        
        // 设置lru
        setLru(hdfsPath);
        
		return true;
	}
    
    /**
     * 得到当前时间 millSeconds str
     * @return
     */
	private String getNowStr() {
		return Calendar.getInstance().getTimeInMillis() + "";
	}
         
 	/**
 	 * key前添加表名前缀
 	 * @param key
 	 * @return
 	 */
 	private String genKey(String tableName, String key) {
         return tableName + ":" + key;
 	}
     
    /**
     * 更新访问时间
     * @param hdfsPath
     * @return
     */
	private boolean updateLru(String hdfsPath) {
        // 先判断是否LRU表中有
		String timeStr = jedis.hget(hdfsCacheLRUTable, hdfsPath);
        if(StringUtils.isEmpty(timeStr)) {
            return false;
        } else {
            // 有, 则时间设为当前
        	jedis.hset(hdfsCacheLRUTable, hdfsPath, getNowStr());
        }
        return true;
	}
    
    /**
     * 设置lru
     * @param hdfsPath
     * @return
     */
	private boolean setLru(String hdfsPath) {
        // 添加到lru中
        jedis.hset(hdfsCacheLRUTable, hdfsPath, getNowStr());
        return true;
	}
}

