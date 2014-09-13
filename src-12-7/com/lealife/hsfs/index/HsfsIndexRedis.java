package com.lealife.hsfs.index;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import redis.clients.jedis.Jedis;

/**
 * 索引机制
 * 使用Redis
 * 两张表, 
 * 	fileId2Block:fileId => hdfsPath
 * 	blockIndex:hdfsPath => {1212: fileId1, 2232:fileId2}
 * @author life
 *
 */
public class HsfsIndexRedis implements HsfsIndexInterface {
	static Jedis jedis = new Jedis("192.168.22.198"); 
        
	static String fileId2BlockTable = "fileId2Block";
	static String blockIndexTable = "blockIndex";
    
    /**
     * 由fileId 得到 hdfsPath
     */
	@Override
	public String getHdfsPath(String fileId) {
        return jedis.get(genKey(fileId2BlockTable ,fileId));
	}
    
    /**
     * 设置fileid => hdfsPath
	 * @deprecated
     */
 	@Deprecated
	@Override
	public void setHdfsPath(String fileId, String hdfsPath) {
        jedis.set(genKey(fileId2BlockTable ,fileId), hdfsPath);
	} 
    /**
     * 批量设置
     * @param fileIds
     * @param hdfsPath
     */
    @Override
 	public void setHdfsPath(Collection<String> fileIds, String hdfsPath) {
        for(String fileId : fileIds) {
        	jedis.set(genKey(fileId2BlockTable ,fileId), hdfsPath);
        }
 	}
     
 	//------------------------
    
    /**
     * 设置块索引
     */
 	@Override
	public void setHdfsIndices(String hdfsPath, Map<Integer, String> indices) {
        String key = genKey(blockIndexTable, hdfsPath);
        for(Map.Entry<Integer, String> eachEntry : indices.entrySet()) {
            jedis.hset(key, eachEntry.getKey() + "", eachEntry.getValue());
        }
 	}
     
 	/**
 	 * 得到块内索引
 	 * @param hdfsPath
 	 * @return
 	 */
 	@Override
 	public Map<Integer, String> getBlockIndices(String hdfsPath) {
         Map<Integer, String> indices = new HashMap<Integer, String>();
         Map<String, String> indicesStr = jedis.hgetAll(genKey(blockIndexTable, hdfsPath));
         if(indicesStr != null) {
             for(Map.Entry<String, String> eachEntry : indicesStr.entrySet()) {
            	 indices.put(Integer.parseInt(eachEntry.getKey()), eachEntry.getValue());
             }
         }
         return indices;
 	}
     
 	//----------------------
     
 	/**
 	 * key前添加表名前缀
 	 * @param key
 	 * @return
 	 */
 	private String genKey(String tableName, String key) {
         return tableName + ":" + key;
 	}
}
