package com.lealife.hsfs;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.lealife.hsfs.cache.HsfsCacheRedis;
import com.lealife.hsfs.cache.HsfsCacheInterface;
import com.lealife.hsfs.index.HsfsIndexRedis;
import com.lealife.hsfs.index.HsfsIndexInterface;
import com.lealife.hsfs.util.FileUtil;

/**
 * hsfs实现
 * 
 * 2013/11/17
 * 改动: 全局索引, Block中只放数据, block内小文件索引放到Redis中, 全局管理.
 * 为什么hdfs总要实现自己管理索引呢? 这种结构化的数据放在成熟的RDMS, NOSQL上管理不是更方便?
 * 
 * @author Life
 *
 */
public class Hsfs implements HsfsProtocal {
	static Logger logger = Logger.getLogger(Hsfs.class);

	/**
	 * hdfs块字节数
     * 30 表示 30M
     * 这里还要定义一个hdfsBlockNum 表示允许的合并空间块个数, 
     * 	不一定超过1个块大小就合并, 超过了 hdfsBlockNum * hdfsBlockSize 才合并
	 */
	static int hdfsBlockSize = 30 * 1024 * 1024;
    static int hdfsBlockNum = 2;
    
    /**
     * 索引
     */
	static HsfsIndexInterface hsfsIndex = new HsfsIndexRedis();
    
	/**
	 * 缓存 getFile cache
	 */
    static HsfsCacheInterface hsfsCache = new HsfsCacheRedis();
	
	/**
	 * hdfs file system
	 */
	static Configuration conf = new Configuration();

	
	/**
	 * 缓存写入到hdfs中
	 */
	private boolean putToHdfs() {
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
        Map<String, byte[]> putFiles = hsfsCache.getPutFiles();
        String fileId = null;
		for(Map.Entry<String, byte[]> each : putFiles.entrySet()) {
			// 第一个index是第一个文件的最后位置
			if(dataPos != 0) {
                indices.put(dataPos, fileId);
				logger.info("index: " + dataPos);
			}
			
			fileId = each.getKey();
			
			byte[] content = each.getValue();
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
			String hdfsPath = "/hsfs/" + tmpFilePath[1];
			getFileSystem().copyFromLocalFile(new Path(tmpFilePath[0] + "/" + tmpFilePath[1]), new Path(hdfsPath));
			
			// 清空put file缓存
			hsfsCache.clearPutFiles(putFiles.keySet());
			
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
			FileUtil.deleteFile(tmpFilePath[0] + "/" + tmpFilePath[1]);
		}
		
		return false;
	}

	/**
	 * 读入文件成byte[], 放fileSize是否超过, 超过, 把之前的写入到hdfs中, 并更新到fileId2PathMap
	 * 不超过, 加入到putFileCache中
	 */
	@Override
	public String put(String filePath) {
		File file = new File(filePath);
		Long length = file.length(); // 字节数
		
		// 超过 把之前的写入到hdfs中
        // 本文件不上传
		if(hsfsCache.getPutFilesSize() + length > hdfsBlockSize * hdfsBlockNum) {
			logger.info("之前的上传到hdfs中...");
			putToHdfs();
		}
		
		logger.info("暂时不上传" + filePath);
		
		// 不超过, 则加入到putFileCache中
		byte[] buffer = null;
        try {
	        if(!file.exists() || file.isDirectory()) {
                System.out.println(filePath);
	            throw new FileNotFoundException();
	        }
	        // 读文件到buffer中 byte[]
	        FileInputStream fis = new FileInputStream(file);  
            ByteArrayOutputStream bos = new ByteArrayOutputStream(1000);  
            byte[] b = new byte[1000];
            int n;
            while ((n = fis.read(b)) != -1) {  
                bos.write(b, 0, n);
            }
            fis.close();
            bos.close();
            buffer = bos.toByteArray();
            
            // 加入到cache中
            String fileId = genFileId();
            hsfsCache.addPutFile(fileId, buffer);
            
            return fileId;
        } catch(Exception e) {
        	e.printStackTrace();
        }
		return null;
	}
	
	/**
	 * 得到文件
	 * 
	 * 文件的位置, 可能在putFileCache, getFileCache, 和hdfs中.
	 * 先putFileCache, 再hsfsCache, 如果没有, 
	 * 	则查fileId2PathMap得到hdfs文件路径, 解析里面的index. 缓存这个文件的所有信息到hsfsCache中
	 */
	@Override
	public byte[] get(String fileId) {
        // 1
		String hdfsPath = getHdfsPath(fileId);
        byte[] fileContent = hsfsCache.getCache(hdfsPath, fileId);
        if(fileContent != null && fileContent.length > 0) {
			logger.info("hsfsCache 中获取");
            return fileContent;
        }
        
        // 2
		logger.info("Hdfs 中获取");
        return getContent(hdfsPath, fileId);
	}

	/**
	 * 暂时不实现, 思路, 得到该id的hdfs文件. 将该文件重新组织即可.
	 * 
	 */
	@Override
	public Boolean delete(String fileId) {
		return null;
	}
	
	/**
	 * 生成fileId
	 * @return
	 */
	private String genFileId() {
		return UUID.randomUUID().toString();
	}
	
	private byte[] int2byte(int res) {
//		byte[] targets = new byte[4];
//
//		targets[0] = (byte) (res & 0xff);// 最低位 
//		targets[1] = (byte) ((res >> 8) & 0xff);// 次低位 
//		targets[2] = (byte) ((res >> 16) & 0xff);// 次高位 
//		targets[3] = (byte) (res >>> 24);// 最高位,无符号右移。 
//		return targets; 
		
		ByteArrayOutputStream buf = new ByteArrayOutputStream();   
		DataOutputStream out = new DataOutputStream(buf);   
		try {
			out.writeInt(res);
			byte[] b = buf.toByteArray();
			out.close();
			buf.close();
			return b;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	} 
	
	 /** 
     * 根据byte数组，生成文件 
     */  
    private String[] genLocalFile(byte[] bytes) {
    	String filePath = "/tmp";
    	String fileName = genFileId();
    	String filePathName = filePath+"/"+fileName;
       
    	if(FileUtil.bytes2File(bytes, filePathName)) {
    		return new String[]{filePath, fileName};
    	}
    	return null;
    }
    
    /**
     * 从fileId得到hdfs文件路径
     * @param fileId
     * @return
     */
    private String getHdfsPath(String fileId) {
        return hsfsIndex.getHdfsPath(fileId);
    }
    
	/**
	 * fileId => hdfsPath
	 * @param fileIds
	 * @param hdfsPath
	 */
	private void setFileId2HdfsPath(Map<Integer, String> indices, String hdfsPath) {

	}
	
	/**
     * 读取文件, 设置缓存, 返回小文件
	 * @param content
	 * @return
	 */
	private byte[] getContent(String hdfsPath, String fileId) {
        Path path = new Path(hdfsPath);
		FSDataInputStream is;
        byte[] content = null;
		try {
            System.out.println(path.toString());
            FileSystem fs = getFileSystem();
			is = fs.open(path);
            FileStatus status = fs.getFileStatus(path);
            content = new byte[Integer.parseInt(String.valueOf(status.getLen()))];
            is.readFully(0, content);
            is.close();
            fs.close();
            
            if(content.length < 8) { // 至少有两个索引, index1, 0
    			return null;
    		}
		} catch (IOException e1) {
			e1.printStackTrace();
			return null;
		}
		
        // 索引, 存储数据位置
        // 得到块的索引
        // Map<Integer, fileId>
		Map<Integer, String> indices = hsfsIndex.getBlockIndices(hdfsPath);
        // 得到索引
        ArrayList<Integer> indexArrayList = new ArrayList<Integer>();
        indexArrayList.addAll(indices.keySet());
        Collections.sort(indexArrayList);
        
		// 4个byte作为一个index
		ByteArrayInputStream bais = new ByteArrayInputStream(content);  
	    DataInputStream dis = new DataInputStream(bais);
	    
	    int dataI = 0;
        Map<String, byte[]> fileCacheMap = new HashMap<String, byte[]>();
	    while(true) {
	    	try {
    			// 读入数据
    			int fileLength = getSFileLength(indexArrayList, dataI);
    			byte[] sf = new byte[fileLength];
    			dis.readFully(sf);
    			
    			fileCacheMap.put(indices.get(indexArrayList.get(dataI)), sf);
    			dataI++;
    			
    			// 数据没了
                // 那么缓存起来, 返回之
    			if(dataI >= indexArrayList.size()) {
                    hsfsCache.setCache(hdfsPath, fileCacheMap);
                    return fileCacheMap.get(fileId);
    			}
			} catch (IOException e) {
				e.printStackTrace();
				return null;
			}
	    }
	}
	
	/**
	 * 得到小文件长度
	 * @param indexList
	 * @param dataI
	 * @return
	 */
	private Integer getSFileLength(ArrayList<Integer> indexList, int dataI) {
		if(indexList == null || indexList.size() == 0) {
			return null;
		}
		
		if(dataI == 0) {
			return indexList.get(0);
		} else {
			return indexList.get(dataI) - indexList.get(dataI - 1); 
		}
	}
    
    /**
     * 得到 HDFS 客户端, 必须 每次都新建一个, 不然会报错 FileSystem is Closed
     * @return
     */
	private FileSystem getFileSystem() { 
    	FileSystem fs;
		try {
			return FileSystem.get(conf);
		} catch (IOException e) {
			e.printStackTrace();
            return null;
		}
	}
}
