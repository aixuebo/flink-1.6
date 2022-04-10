
一、ClusterEntrypoint
在yarn等集群上，任意一个分配的节点上运行，启动applicationMaster的任务逻辑
1.在机器上初始化各种服务。
a.在容器的节点上，针对ip+port开启一个prc服务。
b.HighAvailabilityServices
c.HeartbeatServices
d.BlobServer
e.MetricRegistryImpl
f.ClusterInformation
g.TransientBlobCache
h.ArchivedExecutionGraphStore

2.基于上面提供的服务,就可以开启一个集群应该有的功能了。
private RpcService commonRpcService;
private HighAvailabilityServices haServices;
private BlobServer blobServer;//在applicationmaster上开启一个blob服务,接受客户端请求,返回客户端需要的文件信息;功能:将本地的文件数据 上传到hdfs上;从hdfs上下载数据返回给客户端
private TransientBlobCache transientBlobCache; //定期删除blob数据
private ClusterInformation clusterInformation;//提供blob的ip+port

private ArchivedExecutionGraphStore archivedExecutionGraphStore;//存储job的结果 -- 将结果存储在节点的本地目录下，被Dispatcher中使用
private HeartbeatServices heartbeatServices;
private MetricRegistryImpl metricRegistry;


private Dispatcher dispatcher;
private ResourceManager<?> resourceManager;
private JobManagerMetricGroup jobManagerMetricGroup;
private LeaderRetrievalService dispatcherLeaderRetrievalService;//关注zookeeper的一个path,当leader节点发生变化的时候，会收到通知
private LeaderRetrievalService resourceManagerRetrievalService;//关注zookeeper的一个path,当leader节点发生变化的时候，会收到通知
private WebMonitorEndpoint<?> webMonitorEndpoint;


待搞定的是这些核心能力服务是用在什么地方，如何配合完成集群任务的？？核心方法startClusterComponents看如何启动任务的


