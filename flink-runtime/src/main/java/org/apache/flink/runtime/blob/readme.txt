BlobStore 负责持久化存储用户运行时所需要的jar包

客户端上传文件，并且生成该文件的mk5校验码，保存起来。
客户端需要获取文件时，需要拿到自己的mk5来服务器获取

一、BlobKey  用于标识唯一的文件，因为文件名会有重复,所以用BlobKey标识唯一性。
1.分永久类型key、临时类型key
2.BlobKey会关联一个file文件。
3.BlobKey是md5等算法加密后的check校验码

二、获取文件 -- 通过jobId+blobKey 可以获取文件
boolean get(JobID jobId, BlobKey blobKey, File localFile)

三、BlobClient 客户端，用于与server交互,上传、下载、删除文件

四、BlobServerConnection
用于单独处理一个客户端的连接


五、AbstractBlobCache
避免客户端重复下载同一个文件，对下载过的文件做一层缓存

六、应用
BolbStore 主要负责 BlobServer 本地存储的恢复【JobManager 重启】
