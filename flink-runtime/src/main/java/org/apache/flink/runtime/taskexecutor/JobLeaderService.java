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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.jobmaster.JMTMRegistrationSuccess;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.registration.RegisteredRpcConnection;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.registration.RetryingRegistration;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

/**
 * This service has the responsibility to monitor the job leaders (the job manager which is leader
 * for a given job) for all registered jobs. Upon gaining leadership for a job and detection by the
 * job leader service, the service tries to establish a connection to the job leader. After
 * successfully establishing a connection, the job leader listener is notified about the new job
 * leader and its connection. In case that a job leader loses leadership, the job leader listener
 * is notified as well.
 *
 * 管理所有的job Leader,与每一个job leader保持连接关系的服务
 */
public class JobLeaderService {

	private static final Logger LOG = LoggerFactory.getLogger(JobLeaderService.class);

	/** Self's location, used for the job manager connection. */
	private final TaskManagerLocation ownLocation;

	/** The leader retrieval service and listener for each registered job. 管理的job集合,value是job的zookeeper监听器,以及当监听到job的leader发生变化,该如何处理*/
	private final Map<JobID, Tuple2<LeaderRetrievalService, JobLeaderService.JobManagerLeaderListener>> jobLeaderServices;

	/** Internal state of the service. */
	//JobLeaderService服务的状态，CREATED(已创建服务), STARTED(可以稳定对外提供服务), STOPPED(服务停止)
	private volatile JobLeaderService.State state;

	/** Address of the owner of this service. This address is used for the job manager connection. task节点的地址,该地址用于与job manager交互*/
	private String ownerAddress;

	/** Rpc service to use for establishing connections. task节点对外提供的服务*/
	private RpcService rpcService;

	/** High availability services to create the leader retrieval services from. */
	private HighAvailabilityServices highAvailabilityServices;

	/** Job leader listener listening for job leader changes. 当任意job leader发生变化时,触发回调函数*/
	private JobLeaderListener jobLeaderListener;

	public JobLeaderService(TaskManagerLocation location) {
		this.ownLocation = Preconditions.checkNotNull(location);

		// Has to be a concurrent hash map because tests might access this service
		// concurrently via containsJob
		jobLeaderServices = new ConcurrentHashMap<>(4);

		state = JobLeaderService.State.CREATED;

		ownerAddress = null;
		rpcService = null;
		highAvailabilityServices = null;
		jobLeaderListener = null;
	}

	// -------------------------------------------------------------------------------
	// Methods
	// -------------------------------------------------------------------------------

	/**
	 * Start the job leader service with the given services.
	 *
 	 * @param initialOwnerAddress to be used for establishing connections (source address),task节点的对外地址ip
	 * @param initialRpcService to be used to create rpc connections,task节点的对外提供的服务接口
	 * @param initialHighAvailabilityServices to create leader retrieval services for the different jobs
	 * @param initialJobLeaderListener listening for job leader changes 监控当任意一个job leader发生变化,该如何处理
	 */
	public void start(
			final String initialOwnerAddress,
			final RpcService initialRpcService,
			final HighAvailabilityServices initialHighAvailabilityServices,
			final JobLeaderListener initialJobLeaderListener) {

		if (JobLeaderService.State.CREATED != state) {
			throw new IllegalStateException("The service has already been started.");
		} else {
			LOG.info("Start job leader service.");

			this.ownerAddress = Preconditions.checkNotNull(initialOwnerAddress);
			this.rpcService = Preconditions.checkNotNull(initialRpcService);
			this.highAvailabilityServices = Preconditions.checkNotNull(initialHighAvailabilityServices);
			this.jobLeaderListener = Preconditions.checkNotNull(initialJobLeaderListener);
			state = JobLeaderService.State.STARTED;
		}
	}

	/**
	 * Stop the job leader services. This implies stopping all leader retrieval services for the
	 * different jobs and their leader retrieval listeners.
	 *
	 * @throws Exception if an error occurs while stopping the service
	 */
	public void stop() throws Exception {
		LOG.info("Stop job leader service.");

		if (JobLeaderService.State.STARTED == state) {

			for (Tuple2<LeaderRetrievalService, JobLeaderService.JobManagerLeaderListener> leaderRetrievalServiceEntry: jobLeaderServices.values()) {
				LeaderRetrievalService leaderRetrievalService = leaderRetrievalServiceEntry.f0;
				JobLeaderService.JobManagerLeaderListener jobManagerLeaderListener = leaderRetrievalServiceEntry.f1;

				jobManagerLeaderListener.stop();
				leaderRetrievalService.stop();
			}

			jobLeaderServices.clear();
		}

		state = JobLeaderService.State.STOPPED;
	}

	/**
	 * Remove the given job from being monitored by the job leader service.
	 *
	 * @param jobId identifying the job to remove from monitoring
	 * @throws Exception if an error occurred while stopping the leader retrieval service and listener
	 */
	public void removeJob(JobID jobId) throws Exception {
		Preconditions.checkState(JobLeaderService.State.STARTED == state, "The service is currently not running.");

		Tuple2<LeaderRetrievalService, JobLeaderService.JobManagerLeaderListener> entry = jobLeaderServices.remove(jobId);

		if (entry != null) {
			LOG.info("Remove job {} from job leader monitoring.", jobId);

			LeaderRetrievalService leaderRetrievalService = entry.f0;
			JobLeaderService.JobManagerLeaderListener jobManagerLeaderListener = entry.f1;

			leaderRetrievalService.stop();
			jobManagerLeaderListener.stop();
		}
	}

	/**
	 * Add the given job to be monitored. This means that the service tries to detect leaders for
	 * this job and then tries to establish a connection to it.
	 *
	 * @param jobId identifying the job to monitor
	 * @param defaultTargetAddress of the job leader
	 * @throws Exception if an error occurs while starting the leader retrieval service
	 */
	public void addJob(final JobID jobId, final String defaultTargetAddress) throws Exception {
		Preconditions.checkState(JobLeaderService.State.STARTED == state, "The service is currently not running.");

		LOG.info("Add job {} for job leader monitoring.", jobId);

		final LeaderRetrievalService leaderRetrievalService = highAvailabilityServices.getJobManagerLeaderRetriever(
			jobId,
			defaultTargetAddress);

		JobLeaderService.JobManagerLeaderListener jobManagerLeaderListener = new JobManagerLeaderListener(jobId);

		final Tuple2<LeaderRetrievalService, JobManagerLeaderListener> oldEntry = jobLeaderServices.put(jobId, Tuple2.of(leaderRetrievalService, jobManagerLeaderListener));

		if (oldEntry != null) {
			oldEntry.f0.stop();
			oldEntry.f1.stop();
		}

		leaderRetrievalService.start(jobManagerLeaderListener);
	}

	/**
	 * Triggers reconnection to the last known leader of the given job.
	 *
	 * @param jobId specifying the job for which to trigger reconnection
	 * 重新连接jobId --- 重新连接job最后一次知道的leader
	 */
	public void reconnect(final JobID jobId) {
		Preconditions.checkNotNull(jobId, "JobID must not be null.");

		final Tuple2<LeaderRetrievalService, JobManagerLeaderListener> jobLeaderService = jobLeaderServices.get(jobId);

		if (jobLeaderService != null) {
			jobLeaderService.f1.reconnect();
		} else {
			LOG.info("Cannot reconnect to job {} because it is not registered.", jobId);
		}
	}

	/**
	 * Leader listener which tries to establish a connection to a newly detected job leader.
	 * 监听zookeeper上的某一个job leader节点变更情况
	 */
	private final class JobManagerLeaderListener implements LeaderRetrievalListener {

		/** Job id identifying the job to look for a leader. */
		private final JobID jobId;//被监控的jobid

		/** Rpc connection to the job leader. */
		//表示与JobMasterGateway网关接口通信,request的对象是JobMasterId,response对象是JMTMRegistrationSuccess
		private volatile RegisteredRpcConnection<JobMasterId, JobMasterGateway, JMTMRegistrationSuccess> rpcConnection; //task创建与jobManager的leader连接

		/** State of the listener. */
		private volatile boolean stopped;

		/** Leader id of the current job leader. 当前job的leader的uuid是谁 */
		private volatile JobMasterId currentJobMasterId;

		private JobManagerLeaderListener(JobID jobId) {
			this.jobId = Preconditions.checkNotNull(jobId);

			stopped = false;
			rpcConnection = null;
			currentJobMasterId = null;
		}

		public void stop() {
			stopped = true;

			if (rpcConnection != null) {
				rpcConnection.close();
			}
		}

		public void reconnect() {
			if (stopped) {
				LOG.debug("Cannot reconnect because the JobManagerLeaderListener has already been stopped.");
			} else {
				final RegisteredRpcConnection<JobMasterId, JobMasterGateway, JMTMRegistrationSuccess> currentRpcConnection = rpcConnection;

				if (currentRpcConnection != null) {
					if (currentRpcConnection.isConnected()) {

						if (currentRpcConnection.tryReconnect()) {
							// double check for concurrent stop operation
							if (stopped) {
								currentRpcConnection.close();
							}
						} else {
							LOG.debug("Could not reconnect to the JobMaster {}.", currentRpcConnection.getTargetAddress());
						}
					} else {
						LOG.debug("Ongoing registration to JobMaster {}.", currentRpcConnection.getTargetAddress());
					}
				} else {
					LOG.debug("Cannot reconnect to an unknown JobMaster.");
				}
			}
		}

		//当job的新的leader被选举出来了,如何处理
		//参数是新leader的地址 以及 对应的唯一ID标识
		//与新地址建立一个连接
		@Override
		public void notifyLeaderAddress(final @Nullable String leaderAddress, final @Nullable UUID leaderId) {
			if (stopped) {
				LOG.debug("{}'s leader retrieval listener reported a new leader for job {}. " +
					"However, the service is no longer running.", JobLeaderService.class.getSimpleName(), jobId);
			} else {
				//uuid转换成JobMasterId
				final JobMasterId jobMasterId = JobMasterId.fromUuidOrNull(leaderId);

				//打印日志,job的新地址 以及 新的uuid
				LOG.debug("New leader information for job {}. Address: {}, leader id: {}.",
					jobId, leaderAddress, jobMasterId);

				if (leaderAddress == null || leaderAddress.isEmpty()) {//说明还没有leader节点,因此要关闭原来的连接
					// the leader lost leadership but there is no other leader yet.
					if (rpcConnection != null) {
						rpcConnection.close();
					}

					//当监听zookeeper时,发现job的jobMasterId 不再是leader,则回调该函数
					jobLeaderListener.jobManagerLostLeadership(jobId, currentJobMasterId);//说明丢失job的leader节点信息

					currentJobMasterId = jobMasterId;
				} else {

					//与新地址建立一个连接
					currentJobMasterId = jobMasterId;

					if (rpcConnection != null) {
						// check if we are already trying to connect to this leader
						if (!Objects.equals(jobMasterId, rpcConnection.getTargetLeaderId())) {//说明 leader地址都发生变化了,因此要重新连接
							rpcConnection.close();//关闭以前的地址

							//重新连接新的地址
							rpcConnection = new JobManagerRegisteredRpcConnection(
								LOG,
								leaderAddress,
								jobMasterId,
								rpcService.getExecutor());
						}
					} else {
						//task创建与jobManager的leader连接
						rpcConnection = new JobManagerRegisteredRpcConnection(
							LOG,
							leaderAddress,
							jobMasterId,
							rpcService.getExecutor());
					}

					// double check for a concurrent stop operation
					if (stopped) {
						rpcConnection.close();
					} else {
						LOG.info("Try to register at job manager {} with leader id {}.", leaderAddress, leaderId);
						rpcConnection.start();
					}
				}
			}
		}

		@Override
		public void handleError(Exception exception) {
			if (stopped) {
				LOG.debug("{}'s leader retrieval listener reported an exception for job {}. " +
						"However, the service is no longer running.", JobLeaderService.class.getSimpleName(),
					jobId, exception);
			} else {
				jobLeaderListener.handleError(exception);
			}
		}

		/**
		 * Rpc connection for the job manager <--> task manager connection.
		 * 去连接一个job leader节点
		 *
		 * RegisteredRpcConnection<JobMasterId, JobMasterGateway, JMTMRegistrationSuccess>  表示与JobMasterGateway网关接口通信,request的对象是JobMasterId,response对象是JMTMRegistrationSuccess
		 */
		private final class JobManagerRegisteredRpcConnection extends RegisteredRpcConnection<JobMasterId, JobMasterGateway, JMTMRegistrationSuccess> {

			JobManagerRegisteredRpcConnection(
				Logger log,
				String targetAddress,//job leader节点地址
				JobMasterId jobMasterId,//job leader节点ID
				Executor executor) {
				super(log, targetAddress, jobMasterId, executor);
			}

			@Override
			protected RetryingRegistration<JobMasterId, JobMasterGateway, JMTMRegistrationSuccess> generateRegistration() {
				return new JobLeaderService.JobManagerRetryingRegistration(
						LOG,
						rpcService,//task节点的服务端接口
						"JobManager",//请求目标是jobManager
						JobMasterGateway.class,//请求目标接口class
						getTargetAddress(),//目标地址ID
						getTargetLeaderId(),//目标jobmanager的唯一ID
						ownerAddress,//task节点对外的地址
						ownLocation);
			}

			//response成功返回 --- 说明与jobManager的leader节点连接成功
			@Override
			protected void onRegistrationSuccess(JMTMRegistrationSuccess success) {
				// filter out old registration attempts
				if (Objects.equals(getTargetLeaderId(), currentJobMasterId)) {//确认连接成功
					log.info("Successful registration at job manager {} for job {}.", getTargetAddress(), jobId);

					jobLeaderListener.jobManagerGainedLeadership(jobId, getTargetGateway(), success);
				} else {
					log.debug("Encountered obsolete JobManager registration success from {} with leader session ID {}.", getTargetAddress(), getTargetLeaderId());
				}
			}

			//response返回失败
			@Override
			protected void onRegistrationFailure(Throwable failure) {
				// filter out old registration attempts
				if (Objects.equals(getTargetLeaderId(), currentJobMasterId)) {//确认是同一个ID,因此确认是失败了
					log.info("Failed to register at job  manager {} for job {}.", getTargetAddress(), jobId);
					jobLeaderListener.handleError(failure);
				} else {
					log.debug("Obsolete JobManager registration failure from {} with leader session ID {}.", getTargetAddress(), getTargetLeaderId(), failure);
				}
			}
		}
	}

	/**
	 * Retrying registration for the job manager <--> task manager connection.
	 */
	private static final class JobManagerRetryingRegistration
			extends RetryingRegistration<JobMasterId, JobMasterGateway, JMTMRegistrationSuccess> {

		private final String taskManagerRpcAddress;

		private final TaskManagerLocation taskManagerLocation;

		JobManagerRetryingRegistration(
				Logger log,
				RpcService rpcService,
				String targetName,
				Class<JobMasterGateway> targetType,//jobMaster提供的服务
				String targetAddress,
				JobMasterId jobMasterId,
				String taskManagerRpcAddress,
				TaskManagerLocation taskManagerLocation) {
			super(log, rpcService, targetName, targetType, targetAddress, jobMasterId);

			this.taskManagerRpcAddress = taskManagerRpcAddress;
			this.taskManagerLocation = Preconditions.checkNotNull(taskManagerLocation);
		}

		@Override
		protected CompletableFuture<RegistrationResponse> invokeRegistration(
				JobMasterGateway gateway,
				JobMasterId jobMasterId,
				long timeoutMillis) throws Exception {
			return gateway.registerTaskManager(taskManagerRpcAddress, taskManagerLocation, Time.milliseconds(timeoutMillis));
		}
	}

	/**
	 * Internal state of the service.
	 */
	private enum State {
		CREATED, STARTED, STOPPED
	}

	// -----------------------------------------------------------
	// Testing methods
	// -----------------------------------------------------------

	/**
	 * Check whether the service monitors the given job.
	 *
	 * @param jobId identifying the job
	 * @return True if the given job is monitored; otherwise false
	 */
	@VisibleForTesting
	public boolean containsJob(JobID jobId) {
		Preconditions.checkState(JobLeaderService.State.STARTED == state, "The service is currently not running.");

		return jobLeaderServices.containsKey(jobId);
	}
}
