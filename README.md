hsfs
====

hdfs small file system

## 安装
* redis

## 运行
### 运行hdfs
```
start-dfs.sh
```
### 运行redis
```
$> src/redis-server
$> src/redis-cli

常用命令
$> keys *
$> flushAll 清空所有
```

### 运行hsfs

* TestHsfs 测试Hsfs性能
```
testPut();
testGet();

要运行task: hsfs.init(); 见TestHdfs类
```

* TestHdfs 测试hdfs性能
```
testGet();
testPut();
```

* 清空
```
ClearHDFSData 清空hdfs数据
ClearHSFSData 清空hsfs在hdfs的数据
```

## 其它
* hdfs根目录下有hdfs和hsfs两个目录


