package com.lealife.hsfs.cache;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.lealife.hsfs.info.FileId2Data;

public interface HsfsCacheInterface {

	/**
	 * 上传文件, 暂时保存之
	 */
	public abstract void addPutFile(String fileId, byte[] content);

	/**
	 * 得到上传文件总大小
	 * @return
	 */
	public abstract long getPutFilesSize();

	/**
	 * 清空上传文件
	 * 万一有其它进程在上传HDFS时添加了呢?
	 * 慎用
	 */
//	public abstract void clearPutFiles();

	/**
	 * 删除特定文件
	 * @param fileIds
	 */
	public abstract void clearPutFiles(Set<String> fileIds);

	/**
	 * 得到所有的put files
	 * @return
	 */
//	public abstract Map<String, byte[]> getPutFiles();

	/**
	 * 得到缓存
	 * 每次得到缓存, 都要设置下最近使用时间
	 * @param fileId
	 * @return
	 */
	public abstract byte[] getCache(String hdfsPath, String fileId);

	/**
	 * 保存缓存
	 * 要检查缓存大小
	 * @param hdfsPath
	 * @param files
	 * @return
	 */
	public abstract boolean setCache(String hdfsPath, Map<String, byte[]> files);

	List<FileId2Data> getPutFilesByOrder();

}