package com.lealife.hsfs.util;

/**
 * 读取hsfs.properties文件, 得到各种配置
 * 
 * @author life
 *
 */
public class HsfsConfig {
	public static int getHdfsBlockSize() {
		return 30 * 1024 * 1024;
	}
    public static int getHdfsBlockNum() {
    	return 2;
    }
}
