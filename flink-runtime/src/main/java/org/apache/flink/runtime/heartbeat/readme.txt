创建一个心跳管理者服务。需要引入一个监听器，1.当超时如何处理，2.当收到心跳内容如何处理、3.作为发送者，准备什么数据给客户端

一、注册一个新的客户端
1.注册一个jobmanager
jobManagerHeartbeatManager.monitorTarget(jobManagerResourceId, new HeartbeatTarget<Void>() {
			@Override
			public void receiveHeartbeat(ResourceID resourceID, Void payload) { //不需要与jobmanager通信，因此不需要实现
				// the ResourceManager will always send heartbeat requests to the JobManager
				// resourceManager只会发送请求给jobmanager,因此不会接受请求
			}

			@Override
			public void requestHeartbeat(ResourceID resourceID, Void payload) {//向jobmanager发送心跳请求，jobmanager收到信息后，会发到resource manager上心跳。
				jobMasterGateway.heartbeatFromResourceManager(resourceID);
			}
		})

2.注册一个taskmanager
taskManagerHeartbeatManager.monitorTarget(taskExecutorResourceId, new HeartbeatTarget<Void>() {
				@Override
				public void receiveHeartbeat(ResourceID resourceID, Void payload) {//自己不需要接收taskmanager的命令
					// the ResourceManager will always send heartbeat requests to the
					// TaskManager
				}

				@Override
				public void requestHeartbeat(ResourceID resourceID, Void payload) {//向taskmanager发送命令，让taskmanager给自己发送心跳信息
					taskExecutorGateway.heartbeatFromResourceManager(resourceID);
				}
			});

二、客户端心跳超时
1.当jobmanager心跳超时
在内存中删除该job的信息。
调用jobmanager的网关，发送让其网关关闭与该resourcemanager的连接

2.当taskmanager心跳超时
回收资源。
调用TaskManager的网关，发送让其网关关闭与该resourcemanager的连接


三、接收来自客户端发来的心跳
resource manager 目前只接收来自TaskManager、JobManager发来的心跳。不会主动发给他们信息，因此监听器3的方法都不需要实现。
1.当接收来自客户端的心跳后，更新最后接收心跳的时间(说明客户端还活着)。
2.监听器处理方法2.即收到心跳对应的内容后，该如何处理。
a.jobmanager不需要处理客户端发来的信息，只知道他活着就可以。
b.taskmanager不仅只要他活着，还要处理该节点上的slot报告信息。
