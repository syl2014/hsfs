package com.lealife.hsfs.test;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.lealife.hsfs.cache.HsfsCacheInterface;
import com.lealife.hsfs.index.HsfsIndexInterface;
import com.lealife.hsfs.info.FileId2Data;
import com.lealife.hsfs.util.HsfsUtil;
import com.lealife.hsfs.util.HsfsConfig;

public class TestTask extends TimerTask {

    @Override
    public void run() {
    	System.out.println("------------------");
       for(int i = 0; i < 10; ++i) {
    	   System.out.println(i);
           try {
        	   Thread.sleep(1000);
           } catch (InterruptedException e) {
        	   e.printStackTrace();
           }
       }
    }
    
    public static void main(String args[]) {
        Timer timer = new Timer(false);
        timer.schedule(new TestTask(), 0, 1000);
        
        for(int i = 0; i < 1000; ++i) {
     	   System.out.println("main" + i);
            try {
         	   Thread.sleep(1000);
            } catch (InterruptedException e) {
         	   e.printStackTrace();
            }
        }
    }
}