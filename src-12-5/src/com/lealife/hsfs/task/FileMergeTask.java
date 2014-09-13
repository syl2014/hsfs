package com.lealife.hsfs.task;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TimerTask;

import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.lealife.hsfs.cache.HsfsCacheInterface;
import com.lealife.hsfs.index.HsfsIndexInterface;
import com.lealife.hsfs.info.FileId2Data;
import com.lealife.hsfs.util.HsfsUtil;
import com.lealife.hsfs.util.HsfsConfig;

/**
 * 文件定时合并
 * 1s
 * 访问fileSize, 是否>文件合并空间, 
 * 	> 则得到所有文件合并, 执行合并任务
 * @author life
 *
 */
public class FileMergeTask extends TimerTask {
	static Logger logger = Logger.getLogger(FileMergeTask.class);
    
    HsfsCacheInterface hsfsCache;
    HsfsIndexInterface hsfsIndex;
    
    private static int hdfsBlockNum;
    private static int hdfsBlockSize;
    
    private int n = 1;
    
    static {
        hdfsBlockNum = HsfsConfig.getHdfsBlockNum();
        hdfsBlockSize = HsfsConfig.getHdfsBlockSize();
    }
    
    public FileMergeTask(HsfsCacheInterface hsfsCache, HsfsIndexInterface hsfsIndex) {
        this.hsfsCache = hsfsCache;
        this.hsfsIndex = hsfsIndex;
    }
    
    /**
     * 将所以文件合并空间的数据分组, 调用putToHdfs()保存到HDFS中
     * @return
     */
    private boolean putAllToHdfs() {
        // 获取按上传顺序的小文件
    	System.out.println("取出...start");
    	// 这一步, 卡
        List<FileId2Data> putFiles = hsfsCache.getPutFilesByOrder();
        System.out.println("取出...end");
        if(hdfsBlockNum == 1) {
            return putToHdfs(putFiles);
        }
        
        // 分组之
        String fileId = null;
        byte[] file = null;
        int fileSize = 0;
        List<FileId2Data> eachPutFilesGroup = new ArrayList<FileId2Data>();
        for(FileId2Data eachFileId2Data : putFiles) {
            fileId = eachFileId2Data.getFileId();
            file = eachFileId2Data.getData();
            
            // 原因, redis没有fluashAll完全
            // 除了这个情况外, 不会有其它情况
            if(file == null || file.length == 0) {
            	System.out.println("file == null " + fileId);
                continue;
            }
            
            if(fileSize + file.length > hdfsBlockSize) { 
                // 之前的是一组, 加入之
                logger.info("写入到HDFS中, 一组");
                putToHdfs(eachPutFilesGroup);
                
                // 清空这一组数据
                fileSize = 0;
                eachPutFilesGroup = null;
                eachPutFilesGroup = new ArrayList<FileId2Data>();
            } 
            
            fileSize += file.length;
            eachPutFilesGroup.add(new FileId2Data(fileId, file));
        }
        
        // 最后一组
        if(fileSize > 0) {
            putToHdfs(eachPutFilesGroup);
        }
        
    	return true;
    }
	
	/**
	 * 缓存写入到hdfs中
	 */
	private boolean putToHdfs(List<FileId2Data> putFiles) {
		// 索引
        // 位置 => fileId
        Map<Integer, String> indices = new HashMap<Integer, String>();
		
		// 1
		// 合并之后放数据
        // 得到索引
		byte[] dataBytes = new byte[hdfsBlockSize];
		
		int dataPos = 0;
        // 从合并空间中得到所有要合并的文件
        // fileId => byte[]
//        Map<String, byte[]> putFiles = hsfsCache.getPutFiles();
        String fileId = null;
        HashSet<String> fileIds = new HashSet<String>(); // 为了清空cache
        for(FileId2Data eachFileId2Data : putFiles) {
//		for(Map.Entry<String, byte[]> each : putFiles.entrySet()) {
			// 第一个index是第一个文件的最后位置
			if(dataPos != 0) {
                indices.put(dataPos, fileId);
				logger.info("index: " + dataPos);
			}
			
			fileId = eachFileId2Data.getFileId();
            fileIds.add(fileId);
			
			byte[] content = eachFileId2Data.getData();
			System.arraycopy(content, 0, dataBytes, dataPos, content.length);
			dataPos += content.length;
		}
		// 最后一个文件的末尾
		logger.info("index: " + dataPos);
        indices.put(dataPos, fileId);
		
		// 2
		// 上传到hdfs中, 只上传数据, 索引是全局索引
		// 需要先保存成文件
		String[] tmpFilePath = genLocalFile(dataBytes);
		try {
            // 这里都上传到hdfs的/hsfs/目录下, 一旦文件数量多就会有性能问题
            // 建100个文件夹, 这样循环加到各个目录下
			String hdfsPath = "/hsfs/" + HsfsUtil.getNextHsfsPathNum() + "/" + tmpFilePath[1];
			HsfsUtil.getFileSystem().copyFromLocalFile(new Path(tmpFilePath[0] + "/" + tmpFilePath[1]), new Path(hdfsPath));
			
			// 这里也费时, 阻塞
			System.out.println("写入!");
			
			// 清空put file缓存
			hsfsCache.clearPutFiles(fileIds);
			
            // 设置索引
            
            // fileId => hdfsPath
            hsfsIndex.setHdfsPath(indices.values(), hdfsPath);
            // hdfsPath => {00:fileId1}
            hsfsIndex.setHdfsIndices(hdfsPath, indices);
    			
			return true;
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			// 删除本地文件
			HsfsUtil.deleteFile(tmpFilePath[0] + "/" + tmpFilePath[1]);
		}
		
		return false;
	}
    
	 /** 
     * 根据byte数组，生成文件 
     */  
    private String[] genLocalFile(byte[] bytes) {
    	String filePath = "/tmp";
    	String fileName = HsfsUtil.genFileId();
    	String filePathName = filePath+"/"+fileName;
       
    	if(HsfsUtil.bytes2File(bytes, filePathName)) {
    		return new String[]{filePath, fileName};
    	}
    	return null;
    }
    
    @Override
    public void run() {
        logger.info("merger task test....");
        if(hsfsCache.getPutFilesSize() >= hdfsBlockNum * hdfsBlockSize) {
            // 需要合并了
            logger.info("need merge....");
            logger.info("merge start");
            System.out.println("merge start" + n);
            this.putAllToHdfs();
            logger.info("merge end");
            System.out.println("merge end" + n);
            n++;
        } else {
            logger.info("not merge");
        }
    }
    
    public static void main(String args[]) {
//        Timer timer = new Timer(false);
//        timer.schedule(new FileMergeTask(new HsfsCacheRedis(), new HsfsIndexRedis()), 0, 1000);
    }
}