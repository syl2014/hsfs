package com.lealife.hsfs.index;

import java.util.List;
import redis.clients.jedis.Jedis;

/**
 * 索引机制
 * 使用Redis
 * 
	uuid => hdfs路径. 以后要保存到mysql中, 或redis中
	static Map<String, String> fileId2HdfsPath = new HashMap<String, String>();
	// /hsfs/xxx1 => array("xx1", "xx2")
	static Map<String, ArrayList<String>> hdfsPath2FileIdMap = new HashMap<String, ArrayList<String>>(); 
 * @author life
 *
 */
public class HsfsIndexRedis implements HsfsIndexInterface {
	static Jedis jedis = new Jedis("localhost"); 
        
    /**
     * 两张表, fileId2Block : {fileId => hdfsPath}
     * blockIndex: {hdfsPath => [fileId1, fileId2]
     */
	static String fileId2Block = "fileId2Block";
	static String blockIndex = "blockIndex";
    
    /* (non-Javadoc)
	 * @see com.lealife.hsfs.HsfsIndexInterface#getHdfsPath(java.lang.String)
	 */
	@Override
	public String getHdfsPath(String fileId) {
        return jedis.get(genKey(fileId2Block ,fileId));
	}
 	/* (non-Javadoc)
	 * @see com.lealife.hsfs.HsfsIndexInterface#setHdfsPath(java.lang.String, java.lang.String)
	 */
 	@Override
	public String setHdfsPath(String fileId, String hdfsPath) {
        return jedis.set(genKey(fileId2Block ,fileId), hdfsPath);
	} 
    
 	/* (non-Javadoc)
	 * @see com.lealife.hsfs.HsfsIndexInterface#setHdfsIndex(java.lang.String, java.util.List)
	 */
 	@Override
	public void setHdfsIndex(String hdfsPath, List<String> fileIds) {
        String[] strings = new String[fileIds.size()];
        fileIds.toArray(strings);
    	jedis.lpush(genKey(blockIndex, hdfsPath), strings);
 	}
 	/* (non-Javadoc)
	 * @see com.lealife.hsfs.HsfsIndexInterface#getFileId(java.lang.String, int)
	 */
 	@Override
	public String getFileId(String hdfsPath, int i) {
 		return jedis.lindex(genKey(blockIndex, hdfsPath), i);
 	}
     
 	/**
 	 * key前添加表名前缀
 	 * @param key
 	 * @return
 	 */
 	private String genKey(String tableName, String key) {
         return tableName + ":" + key;
 	}
}
