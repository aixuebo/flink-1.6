一、流程
1.准备
资源提交hdfs
2.与集群交互，申请一个容器
申请applicationid、通过内存+1cpu申请ApplicationMaster,该ApplicationMaster与yarn的resourceManager和nodemanager通信。
在ApplicationMaster这个容器上执行jobmanager程序。

3.jobmanager上再次请求新的yarn资源,执行taskmanager任务

4.返回本地一个客户端,可以跟踪ApplicationMaster上的信息。

