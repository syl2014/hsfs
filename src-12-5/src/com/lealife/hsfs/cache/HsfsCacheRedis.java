package com.lealife.hsfs.cache;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.lealife.hsfs.info.FileId2Data;

import redis.clients.jedis.Jedis;

/**
 * 数据缓存机制, 包括:
 * 	put file cache
 * 	缓存从block get后的数据 
 * 
 * 文件合并空间
 * put file:
 * 表名:
 * 	1) putFileCache : {fileId1 => byte[], fileId2 => byte[]}. 一行记录. 
 * 		无序, 需要添加另一张表存储fileId的顺序, 这样清空也要清空另一张表
 * 		1.1) putFileOrder: {fileId1 => 0000001, fileId2 => 0000002...} 一行记录 2013/12/5
 * 	2) putFileSize: 12 字节大小. 一行记录
 * 
 * 文件缓存空间
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
    
	static Jedis jedis = new Jedis("192.168.22.198"); 
	static Jedis jedisCopy = new Jedis("192.168.22.198"); 
    /**
     * 合并空间表
     */
    static String putFileCahceTable = "putFileCache";
	static String putFileCahceOrderTable = "putFileCacheOrder";
	static String putFileCahceSizeTable = "putFileCacheSize";
    
    /**
     * 最多缓存块数
     * 一个block 64M, 那么就有640M
     */
	static int maxCahceBlockSize = 2; // 10 * Block
        
	static String hdfsCahceTable = "hdfsCache";
	static String hdfsCacheLRUTable = "hdfsCacheLRU";
    
    /**
     * 添加到合并空间中
     */
	@Override
	public synchronized void addPutFile(String fileId, byte[] content) {
        jedis.hset(putFileCahceTable.getBytes(), fileId.getBytes(), content);
        jedis.hset(putFileCahceOrderTable, fileId, getNextPutFileCacheOrder()+"");
        int preSize = getPutFilesSize();
        jedis.set(putFileCahceSizeTable, (preSize + content.length) + "");
	}
    
    /**
     * 得到合并空间大小
     * mergeTask调用
     */
	@Override
	public synchronized int getPutFilesSize() {
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
    
    /**
     * 清空合并空间(所有数据)
     * @deprecated
     */
	@Deprecated
	@Override
	public void clearPutFiles() {
        jedis.set(putFileCahceSizeTable, "0");
        jedis.del(putFileCahceTable);
        jedis.del(putFileCahceOrderTable);
	}
    
    /**
     * 清空特定的合并空间数据,fileIds
     */
	@Override
	public synchronized void clearPutFiles(Set<String> fileIds) {
        // 要减少的总大小
        int size = 0;
        int eachFileSize;
        // 这里很占用时间
        for(String fileId : fileIds) {
            eachFileSize = getPutFileSize(fileId); // 得到文件大小
            if(eachFileSize > 0) {
                size += eachFileSize;
                // 删除之
                jedis.hdel(putFileCahceTable, fileId);
                jedis.hdel(putFileCahceOrderTable, fileId);
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
    
    /**
     * 得到合并空间的所有数据
     */
	@Override
	public synchronized Map<String, byte[]> getPutFiles() {
        // fileId是byte[]类型的数据
		Map<byte[], byte[]> putFiles = jedis.hgetAll(putFileCahceTable.getBytes());
        Map<String, byte[]> files = new HashMap<String, byte[]>();
        for(Entry<byte[], byte[]> each: putFiles.entrySet()) {
            files.put(new String(each.getKey()), each.getValue());
        }
        return files;
	}
    
    /**
     * 得到合并空间的所有数据
     * 按时间有序
     * 所有返回List< FileId2Data >
     * 
     * 优化, 使用redisCopy实例读数据
     * 可能有问题, 先取文件数据, 再取文件排序数据, 可能文件数据<排序数据
     */
	@Override
	public List< FileId2Data > getPutFilesByOrder() {
		Map<byte[], byte[]> putFiles;
		Map<String, String> putFilesOrder;
		// 放这里与放上面没什么区别
//		synchronized(this)  {
        // fileId是byte[]类型的数据
			putFiles = jedisCopy.hgetAll(putFileCahceTable.getBytes());
	        // fileId => order(000001)
			putFilesOrder = jedisCopy.hgetAll(putFileCahceOrderTable);
//		}
		// 000001 => fileId
        
        // 按order排序得到List<fileIds>
		List<String> orders = new ArrayList<String>();
        // order => fileId
        Map<String, String> order2FileId = new HashMap<String, String>();
        for(Map.Entry<String, String> eachEntry : putFilesOrder.entrySet()) {
            orders.add(eachEntry.getValue());
            order2FileId.put(eachEntry.getValue(), eachEntry.getKey());
        }
        // asc 升序
        Collections.sort(orders);
        
        ArrayList<FileId2Data> files = new ArrayList<FileId2Data>();
        
        String fileId = null;
        byte[] eachFile = null;
        for(String order : orders) {
            fileId = order2FileId.get(order);
            eachFile = putFiles.get(fileId.getBytes());
            if(eachFile == null || eachFile.length == 0) {
            	System.out.println("可能文件数据<排序数据" + fileId);
            	continue;
            }
            files.add(new FileId2Data(fileId, eachFile));
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
    
    /**
     * 下一个order
     */
    static int putFileCacheOrder = 0;
	private String getNextPutFileCacheOrder() {
        putFileCacheOrder++;
        return String.format("%012d", putFileCacheOrder);
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
        int lruMapSize = lruMap.size();
        
        // 循环判断
        // 正常情况下只有可能遍历一次. 但如果第一次将maxCacheBlockSize 设大, 之后调小, 就有必要循环删除
        // 循环删除性能不高, 但实现简单(使用场景概率小)
        while(lruMapSize >= maxCahceBlockSize) {
            // 如果超过, 按时间排序, 最小的踢掉
            logger.info("set cahce 当前cache大小: " + lruMapSize);
            
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
            
            lruMap.remove(minKey); // 删除之
            lruMapSize--;
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