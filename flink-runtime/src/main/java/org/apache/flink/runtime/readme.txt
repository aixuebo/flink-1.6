ResourceManager(RM)、Dispatcher、JobManager(JM)、jobmaster、TaskManager(TM)、taskexecute关系。

一、Master
Flink 中 Master 节点主要包含三大组件：Flink Resource Manager、Flink Dispatcher 以及为每个运行的 Job 创建一个 JobManager 服务


这里要说明的一点是：通常我们认为 Flink 集群的 master 节点就是 JobManager，slave 节点就是 TaskManager 或者 TaskExecutor（见：Distributed Runtime Environment），这本身是没有什么问题的。
但这里需要强调一下，在本文中集群的 Master 节点暂时就叫做 Master 节点，而负责每个作业调度的服务，这里叫做 JobManager/JobMaster（现在源码的实现中对应的类是 JobMaster）。

集群的 Master 节点的工作范围与 JobManager 的工作范围还是有所不同的，而且 Master 节点的其中一项工作职责就是为每个提交的作业创建一个 JobManager 对象，用来处理这个作业相关协调工作，
比如：task 的调度、Checkpoint 的触发及失败恢复等，JobManager 的内容将会在下篇文章单独讲述，本文主要聚焦 Master 节点除 JobManager 之外的工作。





ResourceManager:
1.管理有哪些JobManager、TaskManager 以及接受心跳
registerJobManager(): 在 ResourceManager 中注册一个 JobManager 对象，一个作业启动后，JobManager 初始化后会调用这个方法；
registerTaskExecutor(): 在 ResourceManager 中注册一个 TaskExecutor（TaskExecutor 实际上就是一个 TaskManager），当一个 TaskManager 启动后，会主动向 ResourceManager 注册；

disconnectTaskManager(): TaskManager 向 ResourceManager 发送一个断开连接的请求；
disconnectJobManager(): JobManager 向 ResourceManager 发送一个断开连接的请求；


2.申请slot
requestSlot(): JobManager 向 ResourceManager 请求 slot 资源；
cancelSlotRequest(): JobManager 向 ResourceManager 发送一个取消 slot 申请的请求；


3.心跳上报
heartbeatFromTaskManager(): 向 ResourceManager 发送来自 TM 的心跳信息；
heartbeatFromJobManager(): 向 ResourceManager 发送来自 JM 的心跳信息；

sendSlotReport(): TaskManager 向 ResourceManager 发送 SlotReport
（SlotReport 包含了这个 TaskExecutor 的所有 slot 状态信息，比如：哪些 slot 是可用的、哪些 slot 是已经被分配的、被分配的 slot 分配到哪些 Job 上了等）；

notifySlotAvailable(): TaskManager 向 ResourceManager 发送一个请求，通知 ResourceManager 某个 slot 现在可用了（TaskManager 端某个 slot 的资源被释放，可以再进行分配了）；

4.管理
deregisterApplication(): 向资源管理系统（比如：yarn、mesos）申请关闭当前的 Flink 集群，一般是在关闭集群的时候调用的；
requestTaskManagerInfo(): 请求当前注册到 ResourceManager 的 TaskManager 的详细信息（返回的类型是 TaskManagerInfo，可以请求的是全部的 TM 列表，也可以是根据某个 ResourceID 请求某个具体的 TM）；
requestResourceOverview(): 向 ResourceManager 请求资源概况，返回的类型是 ResourceOverview，它包括注册的 TaskManager 数量、注册的 slot 数、可用的 slot 数等；
requestTaskManagerMetricQueryServiceAddresses(): 请求 TaskManagerInfo MetricQueryService 的地址信息；
requestTaskManagerFileUpload(): 向 TaskManagerInfo 发送一个文件上传的请求，这里上传的是 TaskManagerInfo 的 LOG/STDOUT 类型的文件，文件会上传到 Blob Server，这里会拿到一个 BlobKey（Blobkey 实际上是文件名的一部分，通过 BlobKey 可以确定这个文件的物理位置信息）；

总结:
a.JobManager向ResourceManager申请资源，ResourceManager返回资源给JobManager
b.JobManager向taskManager发送任务请求，执行任务。
c.taskManager将slot资源信息定期发送给ResourceManager,以及发送slot是否释放，方便ResourceManager分配新的任务给该slot。


