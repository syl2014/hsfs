package com.lealife.hsfs.util;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public class HsfsUtil {
	
	/**
	 * 删除本地文件
	 * @param filePathName
	 * @return
	 */
	public static Boolean deleteFile(String filePathName) {
		File file = new File(filePathName);
		if(file.exists()) {
			return file.delete();
		}
		return false;
	}
	
	/**
	 * bytes数组转成文件
	 */
	public static Boolean bytes2File(byte[] bytes, String filePathName) {
        BufferedOutputStream bos = null;  
        FileOutputStream fos = null;  
        File file = null;
        try {
            File dir = new File(filePathName);  
            if(!dir.exists() && dir.isDirectory()){ // 判断文件目录是否存在
                dir.mkdirs();
            }
            file = new File(filePathName);  
            fos = new FileOutputStream(file);  
            bos = new BufferedOutputStream(fos);  
            bos.write(bytes);
            
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (bos != null) {  
                try {  
                    bos.close();  
                } catch (IOException e1) {  
                    e1.printStackTrace();  
                }
            }
            if (fos != null) {
                try {  
                    fos.close();  
                } catch (IOException e1) {  
                    e1.printStackTrace();  
                }  
            }  
        }
        
        return false;
	}
    
	/**
	 * 生成fileId
	 * @return
	 */
	public static String genFileId() {
		return UUID.randomUUID().toString();
	}
    
    /**
     * 得到 HDFS 客户端, 必须 每次都新建一个, 不然会报错 FileSystem is Closed
     * @return
     */
	static Configuration conf = new Configuration();
	public static FileSystem getFileSystem() { 
    	FileSystem fs;
		try {
			return FileSystem.get(conf);
		} catch (IOException e) {
			e.printStackTrace();
            return null;
		}
	}
    
    
    /**
     * 得到hsfs文件夹编号
     */
    static int pathN = 0;
    /**
     * hsfs在hdfs存储文件的子目录个数, 从1-hsfsPathNum
     */
	final static int hsfsPathNum = 30; // 1-30    
	public static int getNextHsfsPathNum() {
        int n = pathN % hsfsPathNum;
        pathN++;
        return n+1;
	}
}
