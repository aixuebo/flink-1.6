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

package org.apache.flink.runtime.leaderretrieval;

import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Objects;
import java.util.UUID;

/**
 * The counterpart to the {@link org.apache.flink.runtime.leaderelection.ZooKeeperLeaderElectionService}.
 * This implementation of the {@link LeaderRetrievalService} retrieves the current leader which has
 * been elected by the {@link org.apache.flink.runtime.leaderelection.ZooKeeperLeaderElectionService}.
 * The leader address as well as the current leader session ID is retrieved from ZooKeeper.
 *
 * 同步leaderAddress+leaderSessionID
 *
 * 对外提供了一种服务，当监听的path发生变化的时候，则需要实时获取到最新的leader地址。因此通知给下游LeaderRetrievalListener做相应处理
 */
public class ZooKeeperLeaderRetrievalService implements LeaderRetrievalService,
	NodeCacheListener, //node节点的创建、删除、更新监听
	UnhandledErrorListener {
	private static final Logger LOG = LoggerFactory.getLogger(
		ZooKeeperLeaderRetrievalService.class);

	private final Object lock = new Object();

	/** Connection to the used ZooKeeper quorum. */
	private final CuratorFramework client;

	/** Curator recipe to watch changes of a specific ZooKeeper node. */
	private final NodeCache cache;//节点的内容是存储leaderAddress+leaderSessionID

	private final String retrievalPath;//base path

	/** Listener which will be notified about leader changes. */
	private volatile LeaderRetrievalListener leaderListener;//如果leader被更新了,如何更新

	//记录leader是谁
	private String lastLeaderAddress;

	private UUID lastLeaderSessionID;

	private volatile boolean running;

	//zookeeper状态变更
	private final ConnectionStateListener connectionStateListener = new ConnectionStateListener() {
		@Override
		public void stateChanged(CuratorFramework client, ConnectionState newState) {
			handleStateChange(newState);
		}
	};

	/**
	 * Creates a leader retrieval service which uses ZooKeeper to retrieve the leader information.
	 *
	 * @param client Client which constitutes the connection to the ZooKeeper quorum
	 * @param retrievalPath Path of the ZooKeeper node which contains the leader information
	 */
	public ZooKeeperLeaderRetrievalService(CuratorFramework client, String retrievalPath) {
		this.client = Preconditions.checkNotNull(client, "CuratorFramework client");
		this.cache = new NodeCache(client, retrievalPath);
		this.retrievalPath = Preconditions.checkNotNull(retrievalPath);

		this.leaderListener = null;
		this.lastLeaderAddress = null;
		this.lastLeaderSessionID = null;

		running = false;
	}

	//传入监听器,核心目的用于当leader节点发生变化后,从节点如何快速适配
	@Override
	public void start(LeaderRetrievalListener listener) throws Exception {
		Preconditions.checkNotNull(listener, "Listener must not be null.");
		Preconditions.checkState(leaderListener == null, "ZooKeeperLeaderRetrievalService can " +
				"only be started once.");

		LOG.info("Starting ZooKeeperLeaderRetrievalService {}.", retrievalPath);

		synchronized (lock) {
			leaderListener = listener;

			client.getUnhandledErrorListenable().addListener(this);//添加UnhandledErrorListener监听器,监听异常

			cache.getListenable().addListener(this);//监听NodeCacheListener,对应retrievalPath的内容
			cache.start();

			client.getConnectionStateListenable().addListener(connectionStateListener);//监听zookeeper的状态

			running = true;
		}
	}

	@Override
	public void stop() throws Exception {
		LOG.info("Stopping ZooKeeperLeaderRetrievalService {}.", retrievalPath);

		synchronized (lock) {
			if (!running) {
				return;
			}

			running = false;
		}

		client.getUnhandledErrorListenable().removeListener(this);
		client.getConnectionStateListenable().removeListener(connectionStateListener);

		try {
			cache.close();
		} catch (IOException e) {
			throw new Exception("Could not properly stop the ZooKeeperLeaderRetrievalService.", e);
		}
	}

	//节点有变化，即leader有变化  1.获取最新的leader节点地址+uuid,调接口让业务适配新的地址
	@Override
	public void nodeChanged() throws Exception {
		synchronized (lock) {
			if (running) {
				try {
					LOG.debug("Leader node has changed.");

					//更新最新的leaderAddress+leaderSessionID
					ChildData childData = cache.getCurrentData();//节点的内容是存储leaderAddress+leaderSessionID

					//获取最新的leader节点地址+uuid信息
					String leaderAddress;
					UUID leaderSessionID;

					if (childData == null) {
						leaderAddress = null;
						leaderSessionID = null;
					} else {
						byte[] data = childData.getData();

						if (data == null || data.length == 0) {
							leaderAddress = null;
							leaderSessionID = null;
						} else {
							ByteArrayInputStream bais = new ByteArrayInputStream(data);
							ObjectInputStream ois = new ObjectInputStream(bais);

							leaderAddress = ois.readUTF();
							leaderSessionID = (UUID) ois.readObject();
						}
					}

					//说明leader被更新了
					if (!(Objects.equals(leaderAddress, lastLeaderAddress) &&
						Objects.equals(leaderSessionID, lastLeaderSessionID))) {
						LOG.debug(
							"New leader information: Leader={}, session ID={}.",
							leaderAddress,
							leaderSessionID);

						lastLeaderAddress = leaderAddress;
						lastLeaderSessionID = leaderSessionID;
						leaderListener.notifyLeaderAddress(leaderAddress, leaderSessionID);//通知下游,leader被更新了
					}
				} catch (Exception e) {
					leaderListener.handleError(new Exception("Could not handle node changed event.", e));
					throw e;
				}
			} else {
				LOG.debug("Ignoring node change notification since the service has already been stopped.");
			}
		}
	}

	protected void handleStateChange(ConnectionState newState) {
		switch (newState) {
			case CONNECTED:
				LOG.debug("Connected to ZooKeeper quorum. Leader retrieval can start.");
				break;
			case SUSPENDED:
				LOG.warn("Connection to ZooKeeper suspended. Can no longer retrieve the leader from " +
					"ZooKeeper.");
				break;
			case RECONNECTED:
				LOG.info("Connection to ZooKeeper was reconnected. Leader retrieval can be restarted.");
				break;
			case LOST:
				LOG.warn("Connection to ZooKeeper lost. Can no longer retrieve the leader from " +
					"ZooKeeper.");
				break;
		}
	}

	@Override
	public void unhandledError(String s, Throwable throwable) {
		leaderListener.handleError(new FlinkException("Unhandled error in ZooKeeperLeaderRetrievalService:" + s, throwable));
	}
}
