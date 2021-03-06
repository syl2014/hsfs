package com.lealife.hsfs.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class clearHDFSData {
	public static void main(String args[]) {
		try {
			Configuration conf = new Configuration();
			// conf.set("fs.default.name", "hdfs://master:9000");
            
            // 先删除总之
			FileSystem fs = FileSystem.get(conf);
			Path path = new Path("/hdfs/");
            if(fs.exists(path)) {
            	fs.delete(path);
            }
            // 再创建子
			int i = 1;
			while (i <= 30) {
    			path = new Path("/hdfs/" + i);
				fs = FileSystem.get(conf);
				System.out.println(path.toString() + " 不存在, 创建之");
                fs.mkdirs(path);
                ++i;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}