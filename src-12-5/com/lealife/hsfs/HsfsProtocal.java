package com.lealife.hsfs;

public interface HsfsProtocal {
	/**
	 * 添加文件, 返回文件ID
	 * @param filePath
	 * @return
	 */
	String put(String filePath);
	
	/**
	 * 得到文件, 通过fileId
	 * @param fileId
	 * @return
	 */
	byte[] get(String fileId);
	
	/**
	 * 删除文件
	 * @param fileId
	 * @return
	 */
	Boolean delete(String fileId);
}
