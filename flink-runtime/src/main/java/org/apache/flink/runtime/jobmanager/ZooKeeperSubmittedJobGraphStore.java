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

package org.apache.flink.runtime.jobmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.state.RetrievableStateHandle;
import org.apache.flink.runtime.zookeeper.RetrievableStateStorageHelper;
import org.apache.flink.runtime.zookeeper.ZooKeeperStateHandleStore;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * {@link SubmittedJobGraph} instances for JobManagers running in {@link HighAvailabilityMode#ZOOKEEPER}.
 *
 * <p>Each job graph creates ZNode:
 * <pre>
 * +----O /flink/jobgraphs/&lt;job-id&gt; 1 [persistent]
 * .
 * .
 * .
 * +----O /flink/jobgraphs/&lt;job-id&gt; N [persistent]
 * </pre>
 *
 * <p>The root path is watched to detect concurrent modifications in corner situations where
 * multiple instances operate concurrently. The job manager acts as a {@link SubmittedJobGraphListener}
 * to react to such situations.
 *
 * job的实例化--基于zookeeper--完成job信息的增删改查操作
 * 记录每一个job的提交任务对象,即SubmittedJobGraph对象 ---- 可以有回调函数,当job被添加和删除时，给客户端一个回调处理特殊逻辑
 */
public class ZooKeeperSubmittedJobGraphStore implements SubmittedJobGraphStore {

	private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperSubmittedJobGraphStore.class);

	/** Lock to synchronize with the {@link SubmittedJobGraphListener}. */
	private final Object cacheLock = new Object();

	/** Client (not a namespace facade). */
	private final CuratorFramework client;

	/** The set of IDs of all added job graphs. 内存中存在的job集合*/
	private final Set<JobID> addedJobGraphs = new HashSet<>();

	/** Completed checkpoints in ZooKeeper.基于zookeeper，存储SubmittedJobGraph信息内容 */
	private final ZooKeeperStateHandleStore<SubmittedJobGraph> jobGraphsInZooKeeper;

	/**
	 * Cache to monitor all children. This is used to detect races with other instances working
	 * on the same state.缓存&&监听所有的子节点
	 */
	private final PathChildrenCache pathCache;

	/** The full configured base path including the namespace. */
	private final String zooKeeperFullBasePath;

	/** The external listener to be notified on races. */
	//zookeeper的path下任意节点有增加、删除都会接收到通知,因此会调用该方法,通知子实现类做处理
	private SubmittedJobGraphListener jobGraphListener;//监听器,用于job的增加和删除时，发出通知,子类用于实现具体逻辑

	/** Flag indicating whether this instance is running. */
	private boolean isRunning;

	/**
	 * Submitted job graph store backed by ZooKeeper.
	 *
	 * @param client ZooKeeper client
	 * @param currentJobsPath ZooKeeper path for current job graphs
	 * @param stateStorage State storage used to persist the submitted jobs
	 * @throws Exception
	 */
	public ZooKeeperSubmittedJobGraphStore(
			CuratorFramework client,
			String currentJobsPath,//zookeeper上关于job信息记录的路径  key = high-availability.zookeeper.path.jobgraphs
			RetrievableStateStorageHelper<SubmittedJobGraph> stateStorage) throws Exception { //如何存储stage信息 --- stage信息使用文件系统进行存储,反序列化也是从文件系统读取文件后反序列化成stage

		checkNotNull(currentJobsPath, "Current jobs path");
		checkNotNull(stateStorage, "State storage");

		// Keep a reference to the original client and not the namespace facade. The namespace
		// facade cannot be closed.
		this.client = checkNotNull(client, "Curator client");

		// Ensure that the job graphs path exists
		client.newNamespaceAwareEnsurePath(currentJobsPath)
				.ensure(client.getZookeeperClient());

		// All operations will have the path as root
		CuratorFramework facade = client.usingNamespace(client.getNamespace() + currentJobsPath);

		this.zooKeeperFullBasePath = client.getNamespace() + currentJobsPath;
		this.jobGraphsInZooKeeper = new ZooKeeperStateHandleStore<>(facade, stateStorage);//基于zookeeper，存储SubmittedJobGraph信息内容

		this.pathCache = new PathChildrenCache(facade, "/", false);
		pathCache.getListenable().addListener(new SubmittedJobGraphsPathCacheListener());//zookeeper的path下任意节点有增加、删除都会接收到通知
	}

	@Override
	public void start(SubmittedJobGraphListener jobGraphListener) throws Exception {
		synchronized (cacheLock) {
			if (!isRunning) {
				this.jobGraphListener = jobGraphListener;

				pathCache.start();

				isRunning = true;
			}
		}
	}

	@Override
	public void stop() throws Exception {
		synchronized (cacheLock) {
			if (isRunning) {
				jobGraphListener = null;

				try {
					Exception exception = null;

					try {
						jobGraphsInZooKeeper.releaseAll();//删除所有zookeeper的锁路径,数据还是存储在zookeeper下的
					} catch (Exception e) {
						exception = e;
					}

					try {
						pathCache.close();
					} catch (Exception e) {
						exception = ExceptionUtils.firstOrSuppressed(e, exception);
					}

					if (exception != null) {
						throw new FlinkException("Could not properly stop the ZooKeeperSubmittedJobGraphStore.", exception);
					}
				} finally {
					isRunning = false;
				}
			}
		}
	}

	//恢复还原给定jobId
	@Override
	@Nullable
	public SubmittedJobGraph recoverJobGraph(JobID jobId) throws Exception {
		checkNotNull(jobId, "Job ID");
		final String path = getPathForJob(jobId);//获取job的path

		LOG.debug("Recovering job graph {} from {}{}.", jobId, zooKeeperFullBasePath, path);

		synchronized (cacheLock) {
			verifyIsRunning();

			boolean success = false;

			try {
				RetrievableStateHandle<SubmittedJobGraph> jobGraphRetrievableStateHandle;

				try {
					jobGraphRetrievableStateHandle = jobGraphsInZooKeeper.getAndLock(path);//获取job信息的序列化与反序列化对象
				} catch (KeeperException.NoNodeException ignored) {
					success = true;
					return null;
				} catch (Exception e) {
					throw new FlinkException("Could not retrieve the submitted job graph state handle " +
						"for " + path + " from the submitted job graph store.", e);
				}
				SubmittedJobGraph jobGraph;

				try {
					jobGraph = jobGraphRetrievableStateHandle.retrieveState();//反序列化
				} catch (ClassNotFoundException cnfe) {
					throw new FlinkException("Could not retrieve submitted JobGraph from state handle under " + path +
						". This indicates that you are trying to recover from state written by an " +
						"older Flink version which is not compatible. Try cleaning the state handle store.", cnfe);
				} catch (IOException ioe) {
					throw new FlinkException("Could not retrieve submitted JobGraph from state handle under " + path +
						". This indicates that the retrieved state handle is broken. Try cleaning the state handle " +
						"store.", ioe);
				}

				addedJobGraphs.add(jobGraph.getJobId());

				LOG.info("Recovered {}.", jobGraph);

				success = true;
				return jobGraph;
			} finally {
				if (!success) {
					jobGraphsInZooKeeper.release(path);
				}
			}
		}
	}

	//新增/更新一个job
	@Override
	public void putJobGraph(SubmittedJobGraph jobGraph) throws Exception {
		checkNotNull(jobGraph, "Job graph");
		String path = getPathForJob(jobGraph.getJobId());

		LOG.debug("Adding job graph {} to {}{}.", jobGraph.getJobId(), zooKeeperFullBasePath, path);

		boolean success = false;

		while (!success) {
			synchronized (cacheLock) {
				verifyIsRunning();

				int currentVersion = jobGraphsInZooKeeper.exists(path);

				if (currentVersion == -1) {//新增
					try {
						jobGraphsInZooKeeper.addAndLock(path, jobGraph);//存储jobGraph的信息到磁盘

						addedJobGraphs.add(jobGraph.getJobId());

						success = true;
					}
					catch (KeeperException.NodeExistsException ignored) {
					}
				}
				else if (addedJobGraphs.contains(jobGraph.getJobId())) {//更新
					try {
						jobGraphsInZooKeeper.replace(path, currentVersion, jobGraph);//更新
						LOG.info("Updated {} in ZooKeeper.", jobGraph);

						success = true;
					}
					catch (KeeperException.NoNodeException ignored) {
					}
				}
				else {
					throw new IllegalStateException("Oh, no. Trying to update a graph you didn't " +
							"#getAllSubmittedJobGraphs() or #putJobGraph() yourself before.");
				}
			}
		}

		LOG.info("Added {} to ZooKeeper.", jobGraph);
	}

	//删除一个job --- 物理删除
	@Override
	public void removeJobGraph(JobID jobId) throws Exception {
		checkNotNull(jobId, "Job ID");
		String path = getPathForJob(jobId);

		LOG.debug("Removing job graph {} from {}{}.", jobId, zooKeeperFullBasePath, path);

		synchronized (cacheLock) {
			if (addedJobGraphs.contains(jobId)) {
				if (jobGraphsInZooKeeper.releaseAndTryRemove(path)) {//物理删除文件和路径
					addedJobGraphs.remove(jobId);//内存删除
				} else {
					throw new FlinkException(String.format("Could not remove job graph with job id %s from ZooKeeper.", jobId));
				}
			}
		}

		LOG.info("Removed job graph {} from ZooKeeper.", jobId);
	}

	//释放占用的锁
	@Override
	public void releaseJobGraph(JobID jobId) throws Exception {
		checkNotNull(jobId, "Job ID");
		final String path = getPathForJob(jobId);

		LOG.debug("Releasing locks of job graph {} from {}{}.", jobId, zooKeeperFullBasePath, path);

		synchronized (cacheLock) {
			if (addedJobGraphs.contains(jobId)) {
				jobGraphsInZooKeeper.release(path);//释放占用的锁，但job的文件本身是存在的，还可以继续加载

				addedJobGraphs.remove(jobId);//内存中删除该映射
			}
		}

		LOG.info("Released locks of job graph {} from ZooKeeper.", jobId);
	}

	//返回所有的jobId集合
	@Override
	public Collection<JobID> getJobIds() throws Exception {
		Collection<String> paths;

		LOG.debug("Retrieving all stored job ids from ZooKeeper under {}.", zooKeeperFullBasePath);

		try {
			paths = jobGraphsInZooKeeper.getAllPaths();
		} catch (Exception e) {
			throw new Exception("Failed to retrieve entry paths from ZooKeeperStateHandleStore.", e);
		}

		List<JobID> jobIds = new ArrayList<>(paths.size());

		for (String path : paths) {
			try {
				jobIds.add(jobIdfromPath(path));
			} catch (Exception exception) {
				LOG.warn("Could not parse job id from {}. This indicates a malformed path.", path, exception);
			}
		}

		return jobIds;
	}

	/**
	 * Monitors ZooKeeper for changes.
	 *
	 * <p>Detects modifications from other job managers in corner situations. The event
	 * notifications fire for changes from this job manager as well.
	 * zookeeper的path下任意节点有增加、删除都会接收到通知
	 */
	private final class SubmittedJobGraphsPathCacheListener implements PathChildrenCacheListener {

		@Override
		public void childEvent(CuratorFramework client, PathChildrenCacheEvent event)
				throws Exception {

			if (LOG.isDebugEnabled()) {
				if (event.getData() != null) {
					LOG.debug("Received {} event (path: {})", event.getType(), event.getData().getPath());
				}
				else {
					LOG.debug("Received {} event", event.getType());
				}
			}

			switch (event.getType()) {
				case CHILD_ADDED: {//新增目录
					JobID jobId = fromEvent(event);

					LOG.debug("Received CHILD_ADDED event notification for job {}", jobId);

					synchronized (cacheLock) {
						try {
							if (jobGraphListener != null && !addedJobGraphs.contains(jobId)) {
								try {
									// Whoa! This has been added by someone else. Or we were fast
									// to remove it (false positive).
									jobGraphListener.onAddedJobGraph(jobId);
								} catch (Throwable t) {
									LOG.error("Error in callback", t);
								}
							}
						} catch (Exception e) {
							LOG.error("Error in SubmittedJobGraphsPathCacheListener", e);
						}
					}
				}
				break;

				case CHILD_UPDATED: {
					// Nothing to do
				}
				break;

				case CHILD_REMOVED: {//删除一个目录
					JobID jobId = fromEvent(event);

					LOG.debug("Received CHILD_REMOVED event notification for job {}", jobId);

					synchronized (cacheLock) {
						try {
							if (jobGraphListener != null && addedJobGraphs.contains(jobId)) {
								try {
									// Oh oh. Someone else removed one of our job graphs. Mean!
									jobGraphListener.onRemovedJobGraph(jobId);
								} catch (Throwable t) {
									LOG.error("Error in callback", t);
								}
							}

							break;
						} catch (Exception e) {
							LOG.error("Error in SubmittedJobGraphsPathCacheListener", e);
						}
					}
				}
				break;

				case CONNECTION_SUSPENDED: {
					LOG.warn("ZooKeeper connection SUSPENDING. Changes to the submitted job " +
						"graphs are not monitored (temporarily).");
				}
				break;

				case CONNECTION_LOST: {
					LOG.warn("ZooKeeper connection LOST. Changes to the submitted job " +
						"graphs are not monitored (permanently).");
				}
				break;

				case CONNECTION_RECONNECTED: {
					LOG.info("ZooKeeper connection RECONNECTED. Changes to the submitted job " +
						"graphs are monitored again.");
				}
				break;

				case INITIALIZED: {
					LOG.info("SubmittedJobGraphsPathCacheListener initialized");
				}
				break;
			}
		}

		/**
		 * Returns a JobID for the event's path.
		 */
		private JobID fromEvent(PathChildrenCacheEvent event) {
			return JobID.fromHexString(ZKPaths.getNodeFromPath(event.getData().getPath()));
		}
	}

	/**
	 * Verifies that the state is running.
	 */
	private void verifyIsRunning() {
		checkState(isRunning, "Not running. Forgot to call start()?");
	}

	/**
	 * Returns the JobID as a String (with leading slash).
	 */
	public static String getPathForJob(JobID jobId) {
		checkNotNull(jobId, "Job ID");
		return String.format("/%s", jobId);
	}

	/**
	 * Returns the JobID from the given path in ZooKeeper.
	 *
	 * @param path in ZooKeeper
	 * @return JobID associated with the given path
	 */
	public static JobID jobIdfromPath(final String path) {
		return JobID.fromHexString(path);
	}
}
