package com.lealife.hsfs.test;

import java.util.Map;
import java.util.Set;

import com.lealife.hsfs.util.HsfsConfig;
import com.lealife.hsfs.util.HsfsUtil;
import redis.clients.jedis.Jedis;

public class ClearHSFSCache {
	public static void main(String args[]) {
		Jedis jedis = new Jedis(HsfsConfig.getRedisHost());
		
		Set<String> keys = jedis.keys("hdfsCache*");
		
		for(String key : keys) {
			System.out.println(key);
			jedis.del(key);
		}
		
		jedis.del("hdfsCacheLRU");
	}
}
