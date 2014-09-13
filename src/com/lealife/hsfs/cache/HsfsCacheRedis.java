package com.lealife.hsfs.cache;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.lealife.hsfs.info.FileId2Data;
import com.lealife.hsfs.util.HsfsConfig;

import redis.clients.jedis.Jedis;
import sun.reflect.ReflectionFactory.GetReflectionFactoryAction;

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
    
	static Jedis jedis = new Jedis(HsfsConfig.getRedisHost()); 
	// 为读数据, 清理数据用
	static Jedis jedisCopy = new Jedis(HsfsConfig.getRedisHost()); 
	
    /**
     * 合并空间表
     */
    static String putFileCahceTable = "putFileCache";
	static String putFileCahceOrderTable = "putFileCacheOrder";
	static String putFileCahceSizeTable = "putFileCacheSize";
	// 缓存区, putFileCahceTable, putFileCahceOrderTable都有两个
	static int putFileSeq = 0;
	private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock r = lock.readLock();
    private final Lock w = lock.writeLock();
    
    /**
     * 最多缓存块数
     * 一个block 64M, 那么就有640M
     */
	static int maxCahceBlockSize = 5; // 10 * Block
        
	static String hdfsCahceTable = "hdfsCache";
	static String hdfsCacheLRUTable = "hdfsCacheLRU";
	
	/**
	 * 合并空间
	 * 得到当前写入者表
	 * @return
	 */
	private String getCurWritePutFileCahceTable() {
		return putFileCahceTable + (putFileSeq%2);
	}
	private String getCurWritePutFileCahceOrderTable() {
		return putFileCahceOrderTable + (putFileSeq%2);
	}
	
	/**
	 * 合并空间
	 * 得到当前消费者表
	 * @return
	 */
	private String getCurReadPutFileCahceTable() {
		return putFileCahceTable + ((putFileSeq+1)%2);
	}
	private String getCurReadPutFileCahceOrderTable() {
		return putFileCahceOrderTable + ((putFileSeq+1)%2);
	}
    
    /**
     * 添加到合并空间中
     * synchronized 不写到方法头就会有问题, 为什么?
     */
	@Override
	public synchronized void addPutFile(String fileId, byte[] content) {
		r.lock();
        jedis.hset(getCurWritePutFileCahceTable().getBytes(), fileId.getBytes(), content);
        jedis.hset(getCurWritePutFileCahceOrderTable(), fileId, getNextPutFileCacheOrder()+"");
        r.unlock();
        
        addPutFilesSize(content.length);
	}
    
    /**
     * 得到合并空间大小
     * clearPutFiles()使用
     * addPutFile()使用
     * FileMergeTask使用
     * Hsfs调用(已取消)
     * mergeTask调用
     */
	@Override
	public synchronized long getPutFilesSize() {
        String size = jedis.get(putFileCahceSizeTable);
        if(size == null) {
        	return 0;
        }
		Long redisSize = Long.parseLong(size);
		return redisSize;
	}
	
	// 添加fileSize
	public synchronized void addPutFilesSize(int size) {
		long preSize = getPutFilesSize();
		long endSize = preSize + size;
		if(endSize < 0) {
			endSize = 0;
		}
		
		if(size < 0)
			System.out.println("preSize: " + preSize + "; endsize: " + endSize);
		jedis.set(putFileCahceSizeTable, endSize + "");
	}
    
    /**
     * 得到某个file的大小
     * clearPutFiles()使用
     * 当前读的数据!! getCurReadPutFileCahceTable()
     * @param fileId
     * @return
     */
	private int getPutFileSize(String fileId) {
        byte[] file = jedisCopy.hget(getCurReadPutFileCahceTable().getBytes(), fileId.getBytes());
        if(file == null) {
        	System.out.println("?????????????????????");
        	return 0;
        }
        int size = file.length;
        file = null;
        return size;
	}
	
    /**
     * 清空特定的合并空间数据, fileIds
     * 
     * 清理数据是清理当前读的数据!!
     */
	@Override
	public void clearPutFiles(Set<String> fileIds) {
        // 要减少的总大小
        int size = 0;
        int eachFileSize;
        // 这里很占用时间
        for(String fileId : fileIds) {
            eachFileSize = getPutFileSize(fileId); // 得到文件大小
            if(eachFileSize > 0) {
                size += eachFileSize;
                // 删除之
                jedisCopy.hdel(getCurReadPutFileCahceTable(), fileId);
                jedisCopy.hdel(getCurReadPutFileCahceOrderTable(), fileId);
            } else {
            	System.out.println("有文件为空: " + fileId);
            }
        }
        
        addPutFilesSize(-1 * size);
	}
    
  
    /**
     * 
     * 得到合并空间的所有数据
     * 按时间有序
     * 所有返回List< FileId2Data >
     * 
     * 优化, 使用redisCopy实例读数据
     * 有问题, 先取文件数据, 再取文件排序数据, 可能文件数据<排序数据
     * 
     * 此时, 使用redisCopy实例读数据
     * 	切换缓充区, 将缓冲n++
     * 
     */
	@Override
	public List< FileId2Data > getPutFilesByOrder() {
		w.lock();
		// 切换缓冲区, 写<->读, 之前的写变成现在的读
		putFileSeq++;
		System.out.println("切换缓冲区, 写<->读");
		w.unlock();
		
		Map<byte[], byte[]> putFiles;
		Map<String, String> putFilesOrder;
        // fileId是byte[]类型的数据
		putFiles = jedisCopy.hgetAll(getCurReadPutFileCahceTable().getBytes());
        // fileId => order(000001)
		putFilesOrder = jedisCopy.hgetAll(getCurReadPutFileCahceOrderTable());
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
            	// 这里肯定不会有问题了
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
		// 从两张表中取
		byte[] file = jedis.hget(getCurWritePutFileCahceTable().getBytes(), fileId.getBytes());
		if(file != null && file.length > 0) {
			return file;
		}
		return jedis.hget(getCurReadPutFileCahceTable().getBytes(), fileId.getBytes());
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
            logger.error("踢cache" + minKey);
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