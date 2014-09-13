package com.lealife.hsfs.util;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class TestUtil {
	/**
	 * 得到本地小文件路径
	 * @return
	 */
	public static List<String> getLocalSmallFilesPath() {
		List<String> paths = new ArrayList<String>(); 
		String path = new TestUtil().getClass().getResource("/").getPath() + "small_files/";
		File file = new File(path);
		File[] files = file.listFiles();
		for (int i = 0; i < files.length; i++) {
			if (!files[i].isDirectory()) {
				paths.add(files[i].getPath());
			}
		}
		return paths;
	}
	
	public static void main(String args[]) {
		getLocalSmallFilesPath();
	}
}
