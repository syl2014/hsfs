package com.lealife.hsfs.test;

import java.util.Map;

import com.lealife.hsfs.util.FileUtil;
import com.sun.org.apache.bcel.internal.generic.NEW;

import redis.clients.jedis.Jedis;

public class TestRedis {
	public static void main(String args[]) {
		Jedis jedis = new Jedis("localhost");
//        
//		jedis.set("foo", "life");
//        jedis.hset("fileId", "hdfsPath", "/hsfs/xxx1");
//        jedis.lpush("/hsfs/xxx1", "xxx??");
//        jedis.lset("/hsfs/xxx1", 0, "fileId1");
//        System.out.println(jedis.lindex("/hsfs/xxx1", 0));
//		String value = jedis.get("foo");
//        System.out.println(value);
        
		jedis.hset("life", "k1", "val1");
		System.out.println(jedis.hget("life", "k1"));
		System.out.println(new String(jedis.hget("life".getBytes(), "k1".getBytes())));
        
		System.out.println(jedis.hget("life5", "xx"));
        
        jedis.incrBy("life3", 12);
        System.out.println(jedis.get("life3"));
        
        System.out.println(jedis.keys("life*"));
        
        Map<byte[], byte[]> m = jedis.hgetAll("hdfsCache:/hsfs/d51737a1-5cae-47b1-b44b-0d619ca55aa6".getBytes());
        System.out.println(m.size());
        
        for(Map.Entry<byte[], byte[]> entry : m.entrySet()) {
            byte[] value = entry.getValue();
            if(value.length == 74266) { 
                FileUtil.bytes2File(value, "/Users/life/Desktop/b.gif");
            }
        }
        
	}
}
