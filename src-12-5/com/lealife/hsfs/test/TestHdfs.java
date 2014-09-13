package com.lealife.hsfs.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class TestHdfs {
	public static void main(String args[]) {
		try {
			Configuration conf = new Configuration();
	         // conf.set("fs.default.name", "hdfs://master:9000");
	         Path path = new Path("/hsfs/7e82b8c0-6e6d-4efa-b613-4871b55d37cc");
             int i = 0;
             while(i++ < 3) {
	         FileSystem fs = FileSystem.get(conf);
	         if(fs.exists(path)) {
	             FSDataInputStream is = fs.open(path);
	             FileStatus status = fs.getFileStatus(path);
	             byte[] buffer = new byte[Integer.parseInt(String.valueOf(status.getLen()))];
	             is.readFully(0, buffer);
	             is.close();
	             fs.close();
                 System.out.println("OK");
	        } else {
	        	System.out.println("-------");
	        }
             }
		} catch(Exception e){
			e.printStackTrace();
		}
	}
}
