package com.lealife.hsfs.util;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class FileUtil {
	
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
}
