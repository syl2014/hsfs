package com.lealife.hsfs.test;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import com.lealife.hsfs.Hsfs;

public class TestHsfs {
	
	public static void main(String[] args) {
        String[] files = new String[] {
        	"/Users/life/Desktop/hadoop/hadoop_software/hbase-0.96.0-src.tar.gz",
            "/Users/life/Desktop/ppt/easy.zip",
            "/Users/life/Desktop/青春.gif"
		};
        
		Hsfs hsfs = new Hsfs();
		List<String> fileIds = new ArrayList<String>();
        
		String fileId;
        
		for(int i = 0; i < 20; ++i) {
			fileId = hsfs.put(files[i%3]);
			fileIds.add(fileId);
		}
		
		System.out.println(fileIds);
        Long start = Calendar.getInstance().getTimeInMillis();
       
		for(int i = 0; i < 4; ++i) {
			for(String id : fileIds) {
				byte[] bytes = hsfs.get(id);
                if(bytes == null) {
                    System.out.println(id + " 为空 ???????????????");
                    continue;
                }
				System.out.println(id + " => " + bytes.length);
			}
		}
        
        Long end = Calendar.getInstance().getTimeInMillis();
        System.out.println((end-start)/1000);
	}
}
