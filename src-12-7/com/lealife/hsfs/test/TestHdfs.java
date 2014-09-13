package com.lealife.hsfs.test;

import java.io.IOException;
import java.util.Calendar;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.lealife.hsfs.util.TestUtil;


public class TestHdfs {
	static Configuration conf = new Configuration();
    
    public static void testPut() throws IOException {
        FileSystem fs = FileSystem.get(conf);
        
        List<String> filesPath = TestUtil.getLocalSmallFilesPath(); 
            
        Long start = Calendar.getInstance().getTimeInMillis();
            
        // 10s, 比HSFS快, 为什么?
        // 选择其它小文件试试, 2.63s
        // 101s * 3 > 74s
        // 所以, 选小文件好, 使得hsfs和hdfs交互少, 那么速度就快
        
        // 10,0000的速度, 很慢, 至少10分钟...
		for(int i = 0; i < 10000; ++i) {
            Path src = new Path(filesPath.get(i%50));
            fs.copyFromLocalFile(src, new Path("/hdfs/" + getNextHsfsPathNum() + "/" + UUID.randomUUID().toString()));
//            System.out.println(i);
		}
        
        Long end = Calendar.getInstance().getTimeInMillis();
        System.out.println(1.0*(end-start)/1000);   	
    }
    
    /**
     * 得到hsfs文件夹编号
     */
    static int pathN = 0;
    static int hsfsPathNum = 30;
	private static int getNextHsfsPathNum() {
        int n = pathN % hsfsPathNum;
        pathN++;
        return n+1;
	}
        
	public static void main(String args[]) throws IOException {
        testPut();
	}
}
