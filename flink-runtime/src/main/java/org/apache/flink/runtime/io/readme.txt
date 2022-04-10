flink的IO管理:
1.Network buffers pool:默认使用32k的MemorySegment进行传输，用于task manager批量传输数据。
2.Memory Manager 默认使用32k的MemorySegment进行传输，进行本都计算(sort/hash/cache)使用。
3.Remaining (Free) Heap:剩下的Heap用于存储 TaskManager的数据结构



disk已录视频
核心:通过read/write简单的调用，隐藏背后写入file中的过程。
真实是:
1.用户调用write，先写入内存segment中，然后切换新的segment，写满的segment异步的刷新到文件中。
2.先从文件中读取数据到内存segment中，然后切换新的segment，继续读取文件的数据到新的segment。
用户调用read，从已经读取到的segment中，不断读取数据，因此用户不用关注数据具体存在哪里，只需要不断的read方法即可。



network
1.程序入口
NettyConnectionManager
    org.apache.flink.runtime.taskexecutor.TaskManagerServices --->  new NettyConnectionManager(nettyConfig);
