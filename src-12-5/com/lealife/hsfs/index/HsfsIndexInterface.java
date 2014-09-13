package com.lealife.hsfs.index;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface HsfsIndexInterface {

	/**
	 * uuid => hdfs路径. 以后要保存到mysql中, 或redis中
	 * static Map<String, String> fileId2HdfsPath = new HashMap<String, String>();
	 * @param fileId
	 * @return
	 */
	public abstract String getHdfsPath(String fileId);

	public abstract void setHdfsPath(String fileId, String hdfsPath);
	void setHdfsPath(Collection<String> fileIds, String hdfsPath);
    
	/**
	 * 设置hdfs大文件内小文件的各个索引, 存各小文件fileId
	 * static Map<String, ArrayList<String>> 
	 * 	hdfsPath2FileIdMap = new HashMap<String, ArrayList<String>>(); 
	 */
	void setHdfsIndices(String hdfsPath, Map<Integer, String> indices);

	Map<Integer, String> getBlockIndices(String hdfsPath);
}