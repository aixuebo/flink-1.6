/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.NetworkClientHandler;
import org.apache.flink.runtime.io.network.netty.exception.LocalTransportException;
import org.apache.flink.runtime.io.network.netty.exception.RemoteTransportException;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;

import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFutureListener;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Factory for {@link PartitionRequestClient} instances.
 *
 * <p>Instances of partition requests clients are shared among several {@link RemoteInputChannel}
 * instances.
 * 如何与远程ip建立连接
 */
class PartitionRequestClientFactory {

	private final NettyClient nettyClient;

	private final ConcurrentMap<ConnectionID, Object> clients = new ConcurrentHashMap<ConnectionID, Object>();//已经创建过的连接,key是远程ip地址,value是ConnectingChannel连接中的中间态 或者 PartitionRequestClient已连接对象

	PartitionRequestClientFactory(NettyClient nettyClient) {
		this.nettyClient = nettyClient;
	}

	/**
	 * Atomically establishes a TCP connection to the given remote address and
	 * creates a {@link PartitionRequestClient} instance for this connection.
	 * 与远程地址建立tcp连接
	 */
	PartitionRequestClient createPartitionRequestClient(ConnectionID connectionId) throws IOException, InterruptedException {
		Object entry;
		PartitionRequestClient client = null;

		while (client == null) {
			entry = clients.get(connectionId);

			if (entry != null) {
				// Existing channel or connecting channel
				if (entry instanceof PartitionRequestClient) {//已经连接成功
					client = (PartitionRequestClient) entry;
				}
				else {//处于连接过程中
					ConnectingChannel future = (ConnectingChannel) entry;
					client = future.waitForChannel();

					clients.replace(connectionId, future, client);
				}
			}
			else {
				// No channel yet. Create one, but watch out for a race.还没有创建连接,先创建一个，但要注意race
				// We create a "connecting future" and atomically add it to the map.
				// Only the thread that really added it establishes the channel.
				// The others need to wait on that original establisher's future.
				ConnectingChannel connectingChannel = new ConnectingChannel(connectionId, this);
				Object old = clients.putIfAbsent(connectionId, connectingChannel);//先存储连接中间态

				if (old == null) {//说明没连接过
					nettyClient.connect(connectionId.getAddress()).addListener(connectingChannel);//连接,并且监听连接是否完成

					client = connectingChannel.waitForChannel();//等候完成,返回连接对象

					clients.replace(connectionId, connectingChannel, client);//使用连接对象替换 中间态
				}
				else if (old instanceof ConnectingChannel) {//说明已经在连接中
					client = ((ConnectingChannel) old).waitForChannel();//等候连接返回

					clients.replace(connectionId, old, client);//使用连接对象替换 中间态
				}
				else {
					client = (PartitionRequestClient) old;//使用已经创建好的连接器
				}
			}

			// Make sure to increment the reference count before handing a client
			// out to ensure correct bookkeeping for channel closing.
			if (!client.incrementReferenceCounter()) {
				destroyPartitionRequestClient(connectionId, client);//删除该连接的映射--即该连接已失效
				client = null;
			}
		}

		return client;
	}

	public void closeOpenChannelConnections(ConnectionID connectionId) {
		Object entry = clients.get(connectionId);

		if (entry instanceof ConnectingChannel) {
			ConnectingChannel channel = (ConnectingChannel) entry;

			if (channel.dispose()) {
				clients.remove(connectionId, channel);
			}
		}
	}

	//有多少个服务器连接已经被创建
	int getNumberOfActiveClients() {
		return clients.size();
	}

	/**
	 * Removes the client for the given {@link ConnectionID}.
	 * 删除一个连接映射
	 */
	void destroyPartitionRequestClient(ConnectionID connectionId, PartitionRequestClient client) {
		clients.remove(connectionId, client);
	}

	//代表连接中的中间态 -- 连接过程中,会回调该方法
	private static final class ConnectingChannel implements ChannelFutureListener {

		private final Object connectLock = new Object();

		private final ConnectionID connectionId;//连接哪个远程节点

		private final PartitionRequestClientFactory clientFactory;//连接工厂

		private boolean disposeRequestClient = false;

		private volatile PartitionRequestClient partitionRequestClient;//连接成功

		private volatile Throwable error;//连接失败

		public ConnectingChannel(ConnectionID connectionId, PartitionRequestClientFactory clientFactory) {
			this.connectionId = connectionId;
			this.clientFactory = clientFactory;
		}

		private boolean dispose() {
			boolean result;
			synchronized (connectLock) {
				if (partitionRequestClient != null) {
					result = partitionRequestClient.disposeIfNotUsed();
				}
				else {
					disposeRequestClient = true;
					result = true;
				}

				connectLock.notifyAll();
			}

			return result;
		}

		//参数是连接的对象通道
		private void handInChannel(Channel channel) {
			synchronized (connectLock) {
				try {
					NetworkClientHandler clientHandler = channel.pipeline().get(NetworkClientHandler.class);
					partitionRequestClient = new PartitionRequestClient(
						channel, clientHandler, connectionId, clientFactory);

					if (disposeRequestClient) {
						partitionRequestClient.disposeIfNotUsed();
					}

					connectLock.notifyAll();
				}
				catch (Throwable t) {
					notifyOfError(t);
				}
			}
		}

		//阻塞等待连接成功
		private PartitionRequestClient waitForChannel() throws IOException, InterruptedException {
			synchronized (connectLock) {
				while (error == null && partitionRequestClient == null) {
					connectLock.wait(2000);
				}
			}

			if (error != null) {
				throw new IOException("Connecting the channel failed: " + error.getMessage(), error);
			}

			return partitionRequestClient;
		}

		private void notifyOfError(Throwable error) {
			synchronized (connectLock) {
				this.error = error;
				connectLock.notifyAll();
			}
		}

		@Override
		public void operationComplete(ChannelFuture future) throws Exception {
			if (future.isSuccess()) {//连接成功
				handInChannel(future.channel());
			}
			else if (future.cause() != null) {//连接失败
				notifyOfError(new RemoteTransportException(
						"Connecting to remote task manager + '" + connectionId.getAddress() +
								"' has failed. This might indicate that the remote task " +
								"manager has been lost.",
						connectionId.getAddress(), future.cause()));
			}
			else {
				notifyOfError(new LocalTransportException(
					String.format(
						"Connecting to remote task manager '%s' has been cancelled.",
						connectionId.getAddress()),
					null));
			}
		}
	}
}
