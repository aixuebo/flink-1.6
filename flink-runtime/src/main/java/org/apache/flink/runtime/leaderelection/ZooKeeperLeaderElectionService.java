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

package org.apache.flink.runtime.leaderelection;

import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.UUID;

/**
 * Leader election service for multiple JobManager. The leading JobManager is elected using
 * ZooKeeper. The current leader's address as well as its leader session ID is published via
 * ZooKeeper as well.
 * leader选举服务   在每一个节点都会启动一个该服务
 * 选举服务  该服务持有一个竞选者对象，目标是无论成功、失败、出错都会通知该竞选者，这样服务就比较独立化了
 *
 *
 *
 1.开启竞选start
 2.当选择成功，则调isLeader()，产生一个issuedLeaderSessionID = UUID.randomUUID();
 该uuid通知竞选者。
 竟选择做初始化操作，比如开启master服务，开启成功后，会将uuid传给服务confirmLeaderSessionID()，说明leader确定可以对外服务了。
 3.confirmLeaderSessionID(UUID leaderSessionID)
 当收到竞选者提供的uuid后，更新confirmedLeaderSessionID = leaderSessionID;
 writeLeaderInformation(confirmedLeaderSessionID);并且将uuid和leader对外暴露的master服务地址写到zookeeper上。
 4.当uuid从leader变成非leader,调用notLeader()。
 leaderContender.revokeLeadership();通知竞选者他不是leader了。

 总结:该服务只是在zookeeper上做竞选，一旦成功后，会回调竞选者，竞选者做一些操作后，如果操作都成功，会再回调给服务，通知确定可以服务了，此时就会将master的leader信息写到zookeeper
 上。这样外部任何服务就永远都都知道master是谁了。。master具备的能力是可以随时启动一个服务，恢复元数据的能力。
 */
public class ZooKeeperLeaderElectionService implements LeaderElectionService,
	LeaderLatchListener,//监听leaderLatch是否选中leader,
	NodeCacheListener,//监听leaderPath节点本身被创建，更新或者删除
	UnhandledErrorListener {//出现异常时,如何处理

	private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperLeaderElectionService.class);

	private final Object lock = new Object();

	/** Client to the ZooKeeper quorum. */
	private final CuratorFramework client;

	/** Curator recipe for leader election. */
	private final LeaderLatch leaderLatch;//zookeeper自带的选举器 --- 可以知道该节点是否是leader节点

	/** Curator recipe to watch a given ZooKeeper node for changes. */
	private final NodeCache cache;

	/** ZooKeeper path of the node which stores the current leader information. */
	private final String leaderPath;//选举服务的base path  存储当前leader的信息

	private volatile UUID issuedLeaderSessionID;//发布的id

	private volatile UUID confirmedLeaderSessionID;//确定是ID

	/** The leader contender which applies for leadership. */
	private volatile LeaderContender leaderContender;//竞选者对象

	private volatile boolean running;

	//当zookeeper有变化时，触发
	private final ConnectionStateListener listener = new ConnectionStateListener() {
		@Override
		public void stateChanged(CuratorFramework client, ConnectionState newState) {
			handleStateChange(newState);
		}
	};

	/**
	 * Creates a ZooKeeperLeaderElectionService object.
	 *
	 * @param client Client which is connected to the ZooKeeper quorum
	 * @param latchPath ZooKeeper node path for the leader election latch 存储选举过程中的信息path,以及建立选举时需要创建临时节点的path
	 * @param leaderPath ZooKeeper node path for the node which stores the current leader information 存储leader的信息path
	 */
	public ZooKeeperLeaderElectionService(CuratorFramework client, String latchPath, String leaderPath) {
		this.client = Preconditions.checkNotNull(client, "CuratorFramework client");
		this.leaderPath = Preconditions.checkNotNull(leaderPath, "leaderPath");

		leaderLatch = new LeaderLatch(client, latchPath);
		cache = new NodeCache(client, leaderPath);//监听leaderPath节点本身被创建，更新或者删除

		issuedLeaderSessionID = null;
		confirmedLeaderSessionID = null;
		leaderContender = null;

		running = false;
	}

	/**
	 * Returns the current leader session ID or null, if the contender is not the leader.
	 *
	 * @return The last leader session ID or null, if the contender is not the leader
	 */
	public UUID getLeaderSessionID() {
		return confirmedLeaderSessionID;
	}

	//start方法,传入竞选者对象,参与竞选--即服务对外提供接口能力。
	@Override
	public void start(LeaderContender contender) throws Exception {
		Preconditions.checkNotNull(contender, "Contender must not be null.");
		Preconditions.checkState(leaderContender == null, "Contender was already set.");

		LOG.info("Starting ZooKeeperLeaderElectionService {}.", this);

		synchronized (lock) {

			client.getUnhandledErrorListenable().addListener(this);//添加UnhandledErrorListener接口监听器,出现异常时,如何处理

			leaderContender = contender;

			leaderLatch.addListener(this);//添加LeaderLatchListener回调函数,选举有结果了如何通知,监听leaderLatch是否选中leader,
			leaderLatch.start();//开始参与选举

			cache.getListenable().addListener(this);//监听NodeCacheListener回调函数,监听leaderPath的内容是否发生变化
			cache.start();

			client.getConnectionStateListenable().addListener(listener);//监听ConnectionStateListener回调函数,当zookeeper有变化时,监听变化情况

			running = true;
		}
	}

	@Override
	public void stop() throws Exception{
		synchronized (lock) {
			if (!running) {
				return;
			}

			running = false;
			confirmedLeaderSessionID = null;
			issuedLeaderSessionID = null;
		}

		LOG.info("Stopping ZooKeeperLeaderElectionService {}.", this);

		client.getUnhandledErrorListenable().removeListener(this);

		client.getConnectionStateListenable().removeListener(listener);

		Exception exception = null;

		try {
			cache.close();
		} catch (Exception e) {
			exception = ExceptionUtils.firstOrSuppressed(e, exception);
		}

		try {
			leaderLatch.close();
		} catch (Exception e) {
			exception = ExceptionUtils.firstOrSuppressed(e, exception);
		}

		if (exception != null) {
			throw new Exception("Could not properly stop the ZooKeeperLeaderElectionService.", exception);
		}
	}

	//设置leader的sessionId
	@Override
	public void confirmLeaderSessionID(UUID leaderSessionID) {
		if (LOG.isDebugEnabled()) {
			LOG.debug(
				"Confirm leader session ID {} for leader {}.",
				leaderSessionID,
				leaderContender.getAddress());
		}

		Preconditions.checkNotNull(leaderSessionID);

		if (leaderLatch.hasLeadership()) {//该节点必须是leader节点
			// check if this is an old confirmation call
			synchronized (lock) {
				if (running) {
					if (leaderSessionID.equals(this.issuedLeaderSessionID)) {
						confirmedLeaderSessionID = leaderSessionID;
						writeLeaderInformation(confirmedLeaderSessionID);//会触发nodeChanged函数执行
					}
				} else {
					LOG.debug("Ignoring the leader session Id {} confirmation, since the " +
						"ZooKeeperLeaderElectionService has already been stopped.", leaderSessionID);
				}
			}
		} else {
			LOG.warn("The leader session ID {} was confirmed even though the " +
					"corresponding JobManager was not elected as the leader.", leaderSessionID);
		}
	}

	//该sessionId是否是leader的sessionId
	@Override
	public boolean hasLeadership(@Nonnull UUID leaderSessionId) {
		//该节点必须是leader节点，并且id与leaderId相同
		return leaderLatch.hasLeadership() && leaderSessionId.equals(issuedLeaderSessionID);
	}

	//该节点竞选成功,确定是leader  选举成功会调用isLeader方法
	@Override
	public void isLeader() {
		synchronized (lock) {
			if (running) {
				issuedLeaderSessionID = UUID.randomUUID();
				confirmedLeaderSessionID = null;

				if (LOG.isDebugEnabled()) {
					LOG.debug(
						"Grant leadership to contender {} with session ID {}.",
						leaderContender.getAddress(),
						issuedLeaderSessionID);
				}

				leaderContender.grantLeadership(issuedLeaderSessionID);
			} else {
				LOG.debug("Ignoring the grant leadership notification since the service has " +
					"already been stopped.");
			}
		}
	}

	//确定不是leader  ，由leader变为非leader调用notLeader方法
	@Override
	public void notLeader() {
		synchronized (lock) {
			if (running) {
				issuedLeaderSessionID = null;
				confirmedLeaderSessionID = null;

				if (LOG.isDebugEnabled()) {
					LOG.debug("Revoke leadership of {}.", leaderContender.getAddress());
				}

				leaderContender.revokeLeadership();//通知竟选择,你没有竞选成功,接下来如何做,是竞选者自己的事儿
			} else {
				LOG.debug("Ignoring the revoke leadership notification since the service " +
					"has already been stopped.");
			}
		}
	}

	//监听leaderPath节点本身被创建，更新或者删除
	//不管谁更新了zookeeper上的内容,都会立刻被leader节点刷新最新的leader信息
	@Override
	public void nodeChanged() throws Exception {
		try {
			// leaderSessionID is null if the leader contender has not yet confirmed the session ID
			if (leaderLatch.hasLeadership()) { //说明当前节点是leader,将leader信息写入到path内
				synchronized (lock) {
					if (running) {
						if (LOG.isDebugEnabled()) {
							LOG.debug(
								"Leader node changed while {} is the leader with session ID {}.",
								leaderContender.getAddress(),
								confirmedLeaderSessionID);
						}

						if (confirmedLeaderSessionID != null) {
							ChildData childData = cache.getCurrentData();

							if (childData == null) {
								if (LOG.isDebugEnabled()) {
									LOG.debug(
										"Writing leader information into empty node by {}.",
										leaderContender.getAddress());
								}
								writeLeaderInformation(confirmedLeaderSessionID);
							} else {
								byte[] data = childData.getData();

								if (data == null || data.length == 0) {//说明path内容是无,因此写入数据
									// the data field seems to be empty, rewrite information
									if (LOG.isDebugEnabled()) {
										LOG.debug(
											"Writing leader information into node with empty data field by {}.",
											leaderContender.getAddress());
									}
									writeLeaderInformation(confirmedLeaderSessionID);
								} else {//说明path已经有内容了,如果该内容就是leader节点已经写入过的,因此没必要再次重新写入数据了
									ByteArrayInputStream bais = new ByteArrayInputStream(data);
									ObjectInputStream ois = new ObjectInputStream(bais);

									String leaderAddress = ois.readUTF();
									UUID leaderSessionID = (UUID) ois.readObject();

									if (!leaderAddress.equals(this.leaderContender.getAddress()) ||
										(leaderSessionID == null || !leaderSessionID.equals(confirmedLeaderSessionID))) {
										// the data field does not correspond to the expected leader information
										if (LOG.isDebugEnabled()) {
											LOG.debug(
												"Correcting leader information by {}.",
												leaderContender.getAddress());
										}
										writeLeaderInformation(confirmedLeaderSessionID);
									}
								}
							}
						}
					} else {
						LOG.debug("Ignoring node change notification since the service has already been stopped.");
					}
				}
			}
		} catch (Exception e) {
			leaderContender.handleError(new Exception("Could not handle node changed event.", e));
			throw e;
		}
	}

	/**
	 * Writes the current leader's address as well the given leader session ID to ZooKeeper.
	 *
	 * @param leaderSessionID Leader session ID which is written to ZooKeeper
	 * 将确定的sessionId写到zookeeper下
	 *
	 * 当调用到该方法的时候，该节点一定是leader节点
	 */
	protected void writeLeaderInformation(UUID leaderSessionID) {
		// this method does not have to be synchronized because the curator framework client
		// is thread-safe
		try {
			if (LOG.isDebugEnabled()) {
				LOG.debug(
					"Write leader information: Leader={}, session ID={}.",
					leaderContender.getAddress(),
					leaderSessionID);
			}
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(baos);

			oos.writeUTF(leaderContender.getAddress());
			oos.writeObject(leaderSessionID);

			oos.close();

			boolean dataWritten = false;

			while (!dataWritten && leaderLatch.hasLeadership()) {
				Stat stat = client.checkExists().forPath(leaderPath);

				if (stat != null) {
					long owner = stat.getEphemeralOwner();
					long sessionID = client.getZookeeperClient().getZooKeeper().getSessionId();

					if (owner == sessionID) {
						try {
							client.setData().forPath(leaderPath, baos.toByteArray());

							dataWritten = true;
						} catch (KeeperException.NoNodeException noNode) {
							// node was deleted in the meantime
						}
					} else {
						try {
							client.delete().forPath(leaderPath);
						} catch (KeeperException.NoNodeException noNode) {
							// node was deleted in the meantime --> try again
						}
					}
				} else {
					try {
						client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(
								leaderPath,
								baos.toByteArray());

						dataWritten = true;
					} catch (KeeperException.NodeExistsException nodeExists) {
						// node has been created in the meantime --> try again
					}
				}
			}

			if (LOG.isDebugEnabled()) {
				LOG.debug(
					"Successfully wrote leader information: Leader={}, session ID={}.",
					leaderContender.getAddress(),
					leaderSessionID);
			}
		} catch (Exception e) {
			leaderContender.handleError(
					new Exception("Could not write leader address and leader session ID to " +
							"ZooKeeper.", e));
		}
	}

	protected void handleStateChange(ConnectionState newState) {
		switch (newState) {
			case CONNECTED:
				LOG.debug("Connected to ZooKeeper quorum. Leader election can start.");
				break;
			case SUSPENDED:
				LOG.warn("Connection to ZooKeeper suspended. The contender " + leaderContender.getAddress()
					+ " no longer participates in the leader election.");
				break;
			case RECONNECTED:
				LOG.info("Connection to ZooKeeper was reconnected. Leader election can be restarted.");
				break;
			case LOST:
				// Maybe we have to throw an exception here to terminate the JobManager
				LOG.warn("Connection to ZooKeeper lost. The contender " + leaderContender.getAddress()
					+ " no longer participates in the leader election.");
				break;
		}
	}

	@Override
	public void unhandledError(String message, Throwable e) {
		leaderContender.handleError(new FlinkException("Unhandled error in ZooKeeperLeaderElectionService: " + message, e));
	}

	@Override
	public String toString() {
		return "ZooKeeperLeaderElectionService{" +
			"leaderPath='" + leaderPath + '\'' +
			'}';
	}
}
