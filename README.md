hsfs
====

hdfs small file system

## 安装redis
需要安装到linux(mac)上
```
Installation

Download, extract and compile Redis with:

$ wget http://download.redis.io/releases/redis-2.6.16.tar.gz
$ tar xzf redis-2.6.16.tar.gz
$ cd redis-2.6.16
$ make

The binaries that are now compiled are available in the src directory. Run Redis with:

$ src/redis-server

You can interact with Redis using the built-in client:

$ src/redis-cli
redis> set foo bar
OK
redis> get foo
“bar"
```

## 配置
见类HsfsConfig.java, 主要配置redisHost, 默认是localhost

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
testPut(); // 需要启动task
testGet();

要运行task: hsfs.init(); 见TestHdfs类, 默认已启动
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
