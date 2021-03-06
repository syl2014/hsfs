package com.lealife.hsfs.test;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import redis.clients.jedis.Jedis;

import com.lealife.hsfs.Hsfs;
import com.lealife.hsfs.util.TestUtil;
/**
 * 测试, 
 * 开启hdfs, redis
 * src/redis-server
 * src/redis-cli
     * keys *
     * flushAll 清空所有
 * @author life
 */

public class TestHsfs {
	static Jedis jedis = null; // new Jedis("192.168.22.198"); 
    static String fileIdsTable = "fileIds";
    
	static List<String> fileIds = new ArrayList<String>();
    
	static Hsfs hsfs;
    static {
        hsfs = new Hsfs();
    	hsfs.init();
    }
    
    public static void testPut() {
    	List<String> filesPath = TestUtil.getLocalSmallFilesPath(); 

		String fileId;

        Long start = Calendar.getInstance().getTimeInMillis();
        // 5000 7.055 -> 10000 15s -> 100000 150s
        // 不用后台task 10.883
        
        // 10000 不用后台task 19.004
        // 使用 13.775; 10,0000:123.647
		for(int i = 0; i < 10000; ++i) {
			fileId = hsfs.put(filesPath.get(i%50));
			// 把 fileIds加到redis中, 以后好分析下载用
            // jedis.lpush("fileIds", fileId);
			// fileIds.add(fileId);
			System.out.println(i);
		}
        
        Long end = Calendar.getInstance().getTimeInMillis();
        System.out.println("--------------------------------------------------------------------------");   	
        System.out.println(1.0*(end-start)/1000);
    }
    
    private static void testGet() {
        Long start = Calendar.getInstance().getTimeInMillis();
        
        long fileIdsLen = jedis.llen(fileIdsTable);
        List<String> fileIdsList = jedis.lrange(fileIdsTable, 0, fileIdsLen);
        
		System.out.println(fileIds.size() == fileIdsLen);
        
		for(int i = 0; i < 1; ++i) {
//			for(String id : fileIds) {
			for(String id : fileIdsList) {
				byte[] bytes = hsfs.get(id);
                if(bytes == null) {
                    System.out.println(id + " 为空 ???????????????");
                    continue;
                }
//                if(bytes.length == 74266) {
//                	FileUtil.bytes2File(bytes, "/Users/life/Desktop/a1.gif");
//                }
				System.out.println(id + " => " + bytes.length);
			}
		}
        
        Long end = Calendar.getInstance().getTimeInMillis();
        System.out.println(1.0*(end-start)/1000);   	
    }
    
    public static void getOne(String fileId) {
    	byte[] bytes = hsfs.get(fileId);
    	System.out.println(bytes.length);
    }
    
	public static void main(String[] args) {
        testPut();
//        testGet();
//		getOne("8f995ab5-fdb6-4aa8-adb0-45889d581b9f");
   	}
}