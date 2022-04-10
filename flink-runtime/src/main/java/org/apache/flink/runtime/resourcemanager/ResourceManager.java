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

package org.apache.flink.runtime.resourcemanager;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.blob.TransientBlobKey;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.messages.InfoMessage;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceIDRetrievable;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.HeartbeatListener;
import org.apache.flink.runtime.heartbeat.HeartbeatManager;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.heartbeat.HeartbeatTarget;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.instance.HardwareDescription;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.jobmaster.JobMaster;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.JobMasterRegistrationSuccess;
import org.apache.flink.runtime.leaderelection.LeaderContender;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.dump.MetricQueryService;
import org.apache.flink.runtime.metrics.groups.JobManagerMetricGroup;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.resourcemanager.exceptions.ResourceManagerException;
import org.apache.flink.runtime.resourcemanager.exceptions.UnknownTaskExecutorException;
import org.apache.flink.runtime.resourcemanager.registration.JobManagerRegistration;
import org.apache.flink.runtime.resourcemanager.registration.WorkerRegistration;
import org.apache.flink.runtime.resourcemanager.slotmanager.ResourceActions;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManagerException;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerInfo;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.FencedRpcEndpoint;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.taskexecutor.FileType;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TaskExecutorRegistrationSuccess;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * ResourceManager implementation. The resource manager is responsible for resource de-/allocation
 * and bookkeeping.
 *
 * <p>It offers the following methods as part of its rpc interface to interact with him remotely:
 * <ul>
 *     <li>{@link #registerJobManager(JobMasterId, ResourceID, String, JobID, Time)} registers a {@link JobMaster} at the resource manager</li>
 *     <li>{@link #requestSlot(JobMasterId, SlotRequest, Time)} requests a slot from the resource manager</li>
 * </ul>
 */
public abstract class ResourceManager<WorkerType extends ResourceIDRetrievable>
		extends FencedRpcEndpoint<ResourceManagerId>
		implements ResourceManagerGateway,//实现网关
	LeaderContender {//leader竞选者

	public static final String RESOURCE_MANAGER_NAME = "resourcemanager";

	/** Unique id of the resource manager. */
	private final ResourceID resourceId;

	/** Configuration of the resource manager. */
	private final ResourceManagerConfiguration resourceManagerConfiguration;

	/** All currently registered JobMasterGateways scoped by JobID. */
	private final Map<JobID, JobManagerRegistration> jobManagerRegistrations;

	/** All currently registered JobMasterGateways scoped by ResourceID.所有注册的jobmanager对象 */
	private final Map<ResourceID, JobManagerRegistration> jmResourceIdRegistrations;

	/** Service to retrieve the job leader ids. 如何为每一个job分配leader节点,以及通知最新leader节点是谁 --- 理每一个job的leader*/
	private final JobLeaderIdService jobLeaderIdService;

	/** All currently registered TaskExecutors with there framework specific worker information.
	 * key是具体的taskmanager资源id,value是taskExecutor从节点资源统计对象
	 **/
	private final Map<ResourceID, WorkerRegistration<WorkerType>> taskExecutors;

	/** High availability services for leader retrieval and election. */
	private final HighAvailabilityServices highAvailabilityServices;

	/** The heartbeat manager with task managers. */
	private final HeartbeatManager<SlotReport, Void> taskManagerHeartbeatManager;

	/** The heartbeat manager with job managers. */
	private final HeartbeatManager<Void, Void> jobManagerHeartbeatManager;

	/** Registry to use for metrics. */
	private final MetricRegistry metricRegistry;

	/** Fatal error handler. */
	private final FatalErrorHandler fatalErrorHandler;

	/** The slot manager maintains the available slots. */
	private final SlotManager slotManager;

	private final ClusterInformation clusterInformation;//集群信息

	private final JobManagerMetricGroup jobManagerMetricGroup;

	/** The service to elect a ResourceManager leader. */
	private LeaderElectionService leaderElectionService;

	/** All registered listeners for status updates of the ResourceManager.
	 * 期待resourceManager可以向address发送信息。已经与远程address地址建立好连接，可以发送信息给address节点
	 * key是address
	 **/
	private ConcurrentMap<String, InfoMessageListenerRpcGateway> infoMessageListeners;

	/**
	 * Represents asynchronous state clearing work.
	 *
	 * @see #clearStateAsync()
	 * @see #clearStateInternal()
	 */
	private CompletableFuture<Void> clearStateFuture = CompletableFuture.completedFuture(null);

	public ResourceManager(
			RpcService rpcService,
			String resourceManagerEndpointId,//识别出resourceManager的唯一标识
			ResourceID resourceId,
			ResourceManagerConfiguration resourceManagerConfiguration,
			HighAvailabilityServices highAvailabilityServices,
			HeartbeatServices heartbeatServices,
			SlotManager slotManager,
			MetricRegistry metricRegistry,
			JobLeaderIdService jobLeaderIdService,//如何为每一个job分配leader节点,以及通知最新leader节点是谁
			ClusterInformation clusterInformation,//集群信息
			FatalErrorHandler fatalErrorHandler,
			JobManagerMetricGroup jobManagerMetricGroup) {

		super(rpcService, resourceManagerEndpointId);

		this.resourceId = checkNotNull(resourceId);
		this.resourceManagerConfiguration = checkNotNull(resourceManagerConfiguration);
		this.highAvailabilityServices = checkNotNull(highAvailabilityServices);
		this.slotManager = checkNotNull(slotManager);
		this.metricRegistry = checkNotNull(metricRegistry);
		this.jobLeaderIdService = checkNotNull(jobLeaderIdService);
		this.clusterInformation = checkNotNull(clusterInformation);
		this.fatalErrorHandler = checkNotNull(fatalErrorHandler);
		this.jobManagerMetricGroup = checkNotNull(jobManagerMetricGroup);

		this.taskManagerHeartbeatManager = heartbeatServices.createHeartbeatManagerSender(
			resourceId,
			new TaskManagerHeartbeatListener(),
			rpcService.getScheduledExecutor(),
			log);

		this.jobManagerHeartbeatManager = heartbeatServices.createHeartbeatManagerSender(
			resourceId,
			new JobManagerHeartbeatListener(),
			rpcService.getScheduledExecutor(),
			log);

		this.jobManagerRegistrations = new HashMap<>(4);
		this.jmResourceIdRegistrations = new HashMap<>(4);
		this.taskExecutors = new HashMap<>(8);
		infoMessageListeners = new ConcurrentHashMap<>(8);
	}



	// ------------------------------------------------------------------------
	//  RPC lifecycle methods
	// ------------------------------------------------------------------------

	@Override
	public void start() throws Exception {
		// start a leader
		super.start();//设置状态为开始

		leaderElectionService = highAvailabilityServices.getResourceManagerLeaderElectionService();//竞选leader的服务

		initialize();

		try {
			leaderElectionService.start(this);//本类就是一个竞争者，因此参与竞争
		} catch (Exception e) {
			throw new ResourceManagerException("Could not start the leader election service.", e);
		}

		try {
			jobLeaderIdService.start(new JobLeaderIdActionsImpl());//管理每一个job的leader
		} catch (Exception e) {
			throw new ResourceManagerException("Could not start the job leader id service.", e);
		}

		registerSlotAndTaskExecutorMetrics();
	}

	@Override
	public CompletableFuture<Void> postStop() {
		Exception exception = null;

		taskManagerHeartbeatManager.stop();

		jobManagerHeartbeatManager.stop();

		try {
			slotManager.close();
		} catch (Exception e) {
			exception = ExceptionUtils.firstOrSuppressed(e, exception);
		}

		try {
			leaderElectionService.stop();
		} catch (Exception e) {
			exception = ExceptionUtils.firstOrSuppressed(e, exception);
		}

		try {
			jobLeaderIdService.stop();
		} catch (Exception e) {
			exception = ExceptionUtils.firstOrSuppressed(e, exception);
		}

		clearStateInternal();

		if (exception != null) {
			return FutureUtils.completedExceptionally(
				new FlinkException("Could not properly shut down the ResourceManager.", exception));
		} else {
			return CompletableFuture.completedFuture(null);
		}
	}

	// ------------------------------------------------------------------------
	//  RPC methods
	// ------------------------------------------------------------------------

	/**
	 * 新增一个job
	 * @param jobMasterId The fencing token for the JobMaster leader
	 * @param jobManagerResourceId
	 * @param jobManagerAddress
	 * @param jobId The Job ID of the JobMaster that registers
	 * @param timeout Timeout for the future to complete
	 * @return
	 */
	@Override
	public CompletableFuture<RegistrationResponse> registerJobManager(
			final JobMasterId jobMasterId,
			final ResourceID jobManagerResourceId,
			final String jobManagerAddress,
			final JobID jobId,
			final Time timeout) {

		checkNotNull(jobMasterId);
		checkNotNull(jobManagerResourceId);
		checkNotNull(jobManagerAddress);
		checkNotNull(jobId);

		//获取该job的leader是谁
		if (!jobLeaderIdService.containsJob(jobId)) {
			try {
				jobLeaderIdService.addJob(jobId);
			} catch (Exception e) {
				ResourceManagerException exception = new ResourceManagerException("Could not add the job " +
					jobId + " to the job id leader service.", e);

					onFatalError(exception);

				log.error("Could not add job {} to job leader id service.", jobId, e);
				return FutureUtils.completedExceptionally(exception);
			}
		}

		log.info("Registering job manager {}@{} for job {}.", jobMasterId, jobManagerAddress, jobId);

		//说明leader节点已经启动，并且知道leader是谁
		CompletableFuture<JobMasterId> jobMasterIdFuture;
		try {
			jobMasterIdFuture = jobLeaderIdService.getLeaderId(jobId);//返回jobId对应的JobMasterId
		} catch (Exception e) {
			// we cannot check the job leader id so let's fail
			// TODO: Maybe it's also ok to skip this check in case that we cannot check the leader id
			ResourceManagerException exception = new ResourceManagerException("Cannot obtain the " +
				"job leader id future to verify the correct job leader.", e);

				onFatalError(exception);

			log.debug("Could not obtain the job leader id future to verify the correct job leader.");
			return FutureUtils.completedExceptionally(exception);
		}

		//连接job的地址，请求某一个jobId(jobMasterId),返回动态代理类
		CompletableFuture<JobMasterGateway> jobMasterGatewayFuture = getRpcService().connect(jobManagerAddress, jobMasterId, JobMasterGateway.class);

		//当leader和网关都完成后，如何操作成新的对象
		CompletableFuture<RegistrationResponse> registrationResponseFuture = jobMasterGatewayFuture.thenCombineAsync(
			jobMasterIdFuture,
			(JobMasterGateway jobMasterGateway, JobMasterId currentJobMasterId) -> {//参数是leader和网关对象
				if (Objects.equals(currentJobMasterId, jobMasterId)) {//说明leader没有变化，就是参数传递过来的
					return registerJobMasterInternal(
						jobMasterGateway,
						jobId,
						jobManagerAddress,
						jobManagerResourceId);
				} else {
					log.debug("The current JobMaster leader id {} did not match the received " +
						"JobMaster id {}.", jobMasterId, currentJobMasterId);//说明参数不是leader
					return new RegistrationResponse.Decline("Job manager leader id did not match.");
				}
			},
			getMainThreadExecutor());

		// handle exceptions which might have occurred in one of the futures inputs of combine
		return registrationResponseFuture.handleAsync(
			(RegistrationResponse registrationResponse, Throwable throwable) -> {
				if (throwable != null) {
					if (log.isDebugEnabled()) {
						log.debug("Registration of job manager {}@{} failed.", jobMasterId, jobManagerAddress, throwable);
					} else {
						log.info("Registration of job manager {}@{} failed.", jobMasterId, jobManagerAddress);
					}

					return new RegistrationResponse.Decline(throwable.getMessage());
				} else {
					return registrationResponse;
				}
			},
			getRpcService().getExecutor());
	}

	@Override
	public CompletableFuture<RegistrationResponse> registerTaskExecutor(
			final String taskExecutorAddress,//task节点地址
			final ResourceID taskExecutorResourceId,
			final int dataPort,
			final HardwareDescription hardwareDescription,
			final Time timeout) {

		//连接task节点,获取远程节点的代理
		CompletableFuture<TaskExecutorGateway> taskExecutorGatewayFuture = getRpcService().connect(taskExecutorAddress, TaskExecutorGateway.class);

		return taskExecutorGatewayFuture.handleAsync(
			(TaskExecutorGateway taskExecutorGateway, //远程节点代理
			 Throwable throwable) -> {//异常
				if (throwable != null) {
					return new RegistrationResponse.Decline(throwable.getMessage());//注册失败,回复给客户端
				} else {
					return registerTaskExecutorInternal(
						taskExecutorGateway,
						taskExecutorAddress,
						taskExecutorResourceId,
						dataPort,
						hardwareDescription);
				}
			},
			getMainThreadExecutor());
	}

	//task manager定期上报slot情况
	@Override
	public CompletableFuture<Acknowledge> sendSlotReport(ResourceID taskManagerResourceId, InstanceID taskManagerRegistrationId, SlotReport slotReport, Time timeout) {
		final WorkerRegistration<WorkerType> workerTypeWorkerRegistration = taskExecutors.get(taskManagerResourceId);

		if (workerTypeWorkerRegistration.getInstanceID().equals(taskManagerRegistrationId)) {
			slotManager.registerTaskManager(workerTypeWorkerRegistration, slotReport);
			return CompletableFuture.completedFuture(Acknowledge.get());
		} else {
			return FutureUtils.completedExceptionally(new ResourceManagerException(String.format("Unknown TaskManager registration id %s.", taskManagerRegistrationId)));
		}
	}

	//接收到从TaskManager发来的心跳
	@Override
	public void heartbeatFromTaskManager(final ResourceID resourceID, final SlotReport slotReport) {
		taskManagerHeartbeatManager.receiveHeartbeat(resourceID, slotReport);
	}

	//接收到从JobManager发来的心跳
	@Override
	public void heartbeatFromJobManager(final ResourceID resourceID) {
		jobManagerHeartbeatManager.receiveHeartbeat(resourceID, null);
	}

	@Override
	public void disconnectTaskManager(final ResourceID resourceId, final Exception cause) {
		closeTaskManagerConnection(resourceId, cause);
	}

	@Override
	public void disconnectJobManager(final JobID jobId, final Exception cause) {
		closeJobManagerConnection(jobId, cause);
	}

	//一个job manager来申请资源，仅仅用于资源的一个排队，即知道来自哪个节点地址的job来发送资源请求了，但暂时不实施分配，而是先加入到队列等待分配
	@Override
	public CompletableFuture<Acknowledge> requestSlot(
			JobMasterId jobMasterId,//job manager的leader id
			SlotRequest slotRequest,
			final Time timeout) {

		JobID jobId = slotRequest.getJobId();
		JobManagerRegistration jobManagerRegistration = jobManagerRegistrations.get(jobId);//判断job id是否已经在resource manager上注册成功了

		if (null != jobManagerRegistration) {
			if (Objects.equals(jobMasterId, jobManagerRegistration.getJobMasterId())) {//判断job manager的leader节点是否发生变更
				//打印日志，哪个jobid、请求了什么资源、请求的唯一id是什么
				log.info("Request slot with profile {} for job {} with allocation id {}.",
					slotRequest.getResourceProfile(),
					slotRequest.getJobId(),
					slotRequest.getAllocationId());

				try {
					slotManager.registerSlotRequest(slotRequest);
				} catch (SlotManagerException e) {
					return FutureUtils.completedExceptionally(e);
				}

				return CompletableFuture.completedFuture(Acknowledge.get());
			} else {
				return FutureUtils.completedExceptionally(new ResourceManagerException("The job leader's id " +
					jobManagerRegistration.getJobMasterId() + " does not match the received id " + jobMasterId + '.'));//说明leader节点发生变更
			}

		} else {//说明job没有注册到resource manager上
			return FutureUtils.completedExceptionally(new ResourceManagerException("Could not find registered job manager for job " + jobId + '.'));
		}
	}

	@Override
	public void cancelSlotRequest(AllocationID allocationID) {
		// As the slot allocations are async, it can not avoid all redundant slots, but should best effort.
		slotManager.unregisterSlotRequest(allocationID);
	}

	//TaskExecutor通知ResourceManager，任务执行完成，资源变成可用
	@Override
	public void notifySlotAvailable(
			final InstanceID instanceID,
			final SlotID slotId,
			final AllocationID allocationId) {

		final ResourceID resourceId = slotId.getResourceID();
		WorkerRegistration<WorkerType> registration = taskExecutors.get(resourceId);

		if (registration != null) {
			InstanceID registrationId = registration.getInstanceID();

			if (Objects.equals(registrationId, instanceID)) {
				slotManager.freeSlot(slotId, allocationId);
			} else {
				log.debug("Invalid registration id for slot available message. This indicates an" +
					" outdated request.");
			}
		} else {
			log.debug("Could not find registration for resource id {}. Discarding the slot available" +
				"message {}.", resourceId, slotId);
		}
	}

	/**
	 * Registers an info message listener.
	 *
	 * @param address address of infoMessage listener to register to this resource manager
	 *
	 * 反射的方式连接该服务器address的InfoMessageListenerRpcGateway对象，动态道理的方式本地可以直接使用
	 */
	@Override
	public void registerInfoMessageListener(final String address) {
		if (infoMessageListeners.containsKey(address)) {//因为是注册，所以肯定以前不存在
			log.warn("Receive a duplicate registration from info message listener on ({})", address);
		} else {
			//反射的方式连接该服务器address的InfoMessageListenerRpcGateway对象，动态道理的方式本地可以直接使用
			CompletableFuture<InfoMessageListenerRpcGateway> infoMessageListenerRpcGatewayFuture = getRpcService()
				.connect(address, InfoMessageListenerRpcGateway.class);

			infoMessageListenerRpcGatewayFuture.whenCompleteAsync(
				(InfoMessageListenerRpcGateway gateway, Throwable failure) -> {
					if (failure != null) {
						log.warn("Receive a registration from unreachable info message listener on ({})", address);
					} else {
						log.info("Receive a registration from info message listener on ({})", address);
						infoMessageListeners.put(address, gateway);
					}
				},
				getMainThreadExecutor());
		}
	}

	/**
	 * Unregisters an info message listener.
	 *
	 * @param address of the  info message listener to unregister from this resource manager
	 * 取消与address的通信能力
	 */
	@Override
	public void unRegisterInfoMessageListener(final String address) {
		infoMessageListeners.remove(address);
	}

	/**
	 * Cleanup application and shut down cluster.
	 *
	 * @param finalStatus of the Flink application
	 * @param diagnostics diagnostics message for the Flink application or {@code null}
	 */
	@Override
	public CompletableFuture<Acknowledge> deregisterApplication(
			final ApplicationStatus finalStatus,
			@Nullable final String diagnostics) {
		log.info("Shut down cluster because application is in {}, diagnostics {}.", finalStatus, diagnostics);

		try {
			internalDeregisterApplication(finalStatus, diagnostics);
		} catch (ResourceManagerException e) {
			log.warn("Could not properly shutdown the application.", e);
		}

		return CompletableFuture.completedFuture(Acknowledge.get());
	}

	//返回目前有多少个task manager被注册
	@Override
	public CompletableFuture<Integer> getNumberOfRegisteredTaskManagers() {
		return CompletableFuture.completedFuture(taskExecutors.size());
	}

	//返回所有的task manager的信息
	@Override
	public CompletableFuture<Collection<TaskManagerInfo>> requestTaskManagerInfo(Time timeout) {

		final ArrayList<TaskManagerInfo> taskManagerInfos = new ArrayList<>(taskExecutors.size());

		for (Map.Entry<ResourceID, WorkerRegistration<WorkerType>> taskExecutorEntry : taskExecutors.entrySet()) {
			final ResourceID resourceId = taskExecutorEntry.getKey();
			final WorkerRegistration<WorkerType> taskExecutor = taskExecutorEntry.getValue();

			taskManagerInfos.add(
				new TaskManagerInfo(
					resourceId,
					taskExecutor.getTaskExecutorGateway().getAddress(),
					taskExecutor.getDataPort(),
					taskManagerHeartbeatManager.getLastHeartbeatFrom(resourceId),
					slotManager.getNumberRegisteredSlotsOf(taskExecutor.getInstanceID()),
					slotManager.getNumberFreeSlotsOf(taskExecutor.getInstanceID()),
					taskExecutor.getHardwareDescription()));
		}

		return CompletableFuture.completedFuture(taskManagerInfos);
	}

	//请求某一个task manager的信息
	@Override
	public CompletableFuture<TaskManagerInfo> requestTaskManagerInfo(ResourceID resourceId, Time timeout) {

		final WorkerRegistration<WorkerType> taskExecutor = taskExecutors.get(resourceId);

		if (taskExecutor == null) {
			return FutureUtils.completedExceptionally(new UnknownTaskExecutorException(resourceId));
		} else {
			final InstanceID instanceId = taskExecutor.getInstanceID();
			final TaskManagerInfo taskManagerInfo = new TaskManagerInfo(
				resourceId,//task manager的ResourceID
				taskExecutor.getTaskExecutorGateway().getAddress(),//task manager的网关地址
				taskExecutor.getDataPort(),//task manager的网关端口
				taskManagerHeartbeatManager.getLastHeartbeatFrom(resourceId),//上一次task manager的心跳时间戳
				slotManager.getNumberRegisteredSlotsOf(instanceId),//task manager上有多少个slot
				slotManager.getNumberFreeSlotsOf(instanceId),//task manager上有多少个空闲的slot
				taskExecutor.getHardwareDescription());//task manager的硬件描述

			return CompletableFuture.completedFuture(taskManagerInfo);
		}
	}

	//请求集群信息 : 报告有多少个taskManager、有多少个solt、有多少个空闲的slot
	@Override
	public CompletableFuture<ResourceOverview> requestResourceOverview(Time timeout) {
		final int numberSlots = slotManager.getNumberRegisteredSlots();
		final int numberFreeSlots = slotManager.getNumberFreeSlots();

		return CompletableFuture.completedFuture(
			new ResourceOverview(
				taskExecutors.size(),
				numberSlots,
				numberFreeSlots));
	}

	@Override
	public CompletableFuture<Collection<Tuple2<ResourceID, String>>> requestTaskManagerMetricQueryServicePaths(Time timeout) {
		final ArrayList<Tuple2<ResourceID, String>> metricQueryServicePaths = new ArrayList<>(taskExecutors.size());

		for (Map.Entry<ResourceID, WorkerRegistration<WorkerType>> workerRegistrationEntry : taskExecutors.entrySet()) {
			final ResourceID tmResourceId = workerRegistrationEntry.getKey();
			final WorkerRegistration<WorkerType> workerRegistration = workerRegistrationEntry.getValue();
			final String taskManagerAddress = workerRegistration.getTaskExecutorGateway().getAddress();
			final String tmMetricQueryServicePath = taskManagerAddress.substring(0, taskManagerAddress.lastIndexOf('/') + 1) +
				MetricQueryService.METRIC_QUERY_SERVICE_NAME + '_' + tmResourceId.getResourceIdString();

			metricQueryServicePaths.add(Tuple2.of(tmResourceId, tmMetricQueryServicePath));
		}

		return CompletableFuture.completedFuture(metricQueryServicePaths);
	}

	//请求task网关,让task把相关日志上传到blob服务
	@Override
	public CompletableFuture<TransientBlobKey> requestTaskManagerFileUpload(ResourceID taskManagerId, FileType fileType, Time timeout) {
		log.debug("Request file {} upload from TaskExecutor {}.", fileType, taskManagerId);

		final WorkerRegistration<WorkerType> taskExecutor = taskExecutors.get(taskManagerId);

		if (taskExecutor == null) {
			log.debug("Requested file {} upload from unregistered TaskExecutor {}.", fileType, taskManagerId);
			return FutureUtils.completedExceptionally(new UnknownTaskExecutorException(taskManagerId));
		} else {
			return taskExecutor.getTaskExecutorGateway().requestFileUpload(fileType, timeout);
		}
	}

	// ------------------------------------------------------------------------
	//  Internal methods
	// ------------------------------------------------------------------------

	/**
	 * Registers a new JobMaster.
	 *
	 * @param jobMasterGateway to communicate with the registering JobMaster
	 * @param jobId of the job for which the JobMaster is responsible
	 * @param jobManagerAddress address of the JobMaster
	 * @param jobManagerResourceId ResourceID of the JobMaster
	 * @return RegistrationResponse
	 */
	private RegistrationResponse registerJobMasterInternal(
		final JobMasterGateway jobMasterGateway,
		JobID jobId,
		String jobManagerAddress,
		ResourceID jobManagerResourceId) {
		if (jobManagerRegistrations.containsKey(jobId)) {
			JobManagerRegistration oldJobManagerRegistration = jobManagerRegistrations.get(jobId);

			if (Objects.equals(oldJobManagerRegistration.getJobMasterId(), jobMasterGateway.getFencingToken())) {
				// same registration
				log.debug("Job manager {}@{} was already registered.", jobMasterGateway.getFencingToken(), jobManagerAddress);
			} else {
				// tell old job manager that he is no longer the job leader
				disconnectJobManager(
					oldJobManagerRegistration.getJobID(),
					new Exception("New job leader for job " + jobId + " found."));

				JobManagerRegistration jobManagerRegistration = new JobManagerRegistration(
					jobId,
					jobManagerResourceId,
					jobMasterGateway);
				jobManagerRegistrations.put(jobId, jobManagerRegistration);
				jmResourceIdRegistrations.put(jobManagerResourceId, jobManagerRegistration);
			}
		} else {
			// new registration for the job
			JobManagerRegistration jobManagerRegistration = new JobManagerRegistration(
				jobId,
				jobManagerResourceId,
				jobMasterGateway);
			jobManagerRegistrations.put(jobId, jobManagerRegistration);
			jmResourceIdRegistrations.put(jobManagerResourceId, jobManagerRegistration);
		}

		log.info("Registered job manager {}@{} for job {}.", jobMasterGateway.getFencingToken(), jobManagerAddress, jobId);

		//为该jobmanager注册心跳处理类，周期性的向jobmanager发送信息
		jobManagerHeartbeatManager.monitorTarget(jobManagerResourceId, new HeartbeatTarget<Void>() {
			@Override
			public void receiveHeartbeat(ResourceID resourceID, Void payload) { //不需要与jobmanager通信，因此不需要实现
				// the ResourceManager will always send heartbeat requests to the JobManager
				// resourceManager只会发送请求给jobmanager,因此不会接受请求
			}

			//表示接受到来自resource manager的心跳请求，需要回复给resource manager心跳内容
			//参数resourceID是resourcemanager的id
			@Override
			public void requestHeartbeat(ResourceID resourceID, Void payload) {//向jobmanager发送心跳请求，jobmanager收到信息后，会发到resource manager上心跳。
				jobMasterGateway.heartbeatFromResourceManager(resourceID);
			}
		});

		return new JobMasterRegistrationSuccess(
			resourceManagerConfiguration.getHeartbeatInterval().toMilliseconds(),
			getFencingToken(),
			resourceId);
	}

	/**
	 * Registers a new TaskExecutor.
	 *
	 * @param taskExecutorGateway to communicate with the registering TaskExecutor 远程task节点的本地代理
	 * @param taskExecutorAddress address of the TaskExecutor 远程task节点的地址
	 * @param taskExecutorResourceId ResourceID of the TaskExecutor 远程task节点的id
	 * @param dataPort port used for data transfer 数据传输端口
	 * @param hardwareDescription of the registering TaskExecutor 远程task节点的资源情况
	 * @return RegistrationResponse
	 * 注册一个TaskExecutor
	 */
	private RegistrationResponse registerTaskExecutorInternal(
			TaskExecutorGateway taskExecutorGateway,
			String taskExecutorAddress,
			ResourceID taskExecutorResourceId,
			int dataPort,
			HardwareDescription hardwareDescription) {
		WorkerRegistration<WorkerType> oldRegistration = taskExecutors.remove(taskExecutorResourceId);
		if (oldRegistration != null) {//存在注册过的task
			// TODO :: suggest old taskExecutor to stop itself
			log.debug("Replacing old registration of TaskExecutor {}.", taskExecutorResourceId);

			// remove old task manager registration from slot manager
			slotManager.unregisterTaskManager(oldRegistration.getInstanceID());
		}

		final WorkerType newWorker = workerStarted(taskExecutorResourceId);

		if (newWorker == null) {
			log.warn("Discard registration from TaskExecutor {} at ({}) because the framework did " +
				"not recognize it", taskExecutorResourceId, taskExecutorAddress);
			return new RegistrationResponse.Decline("unrecognized TaskExecutor");
		} else {
			WorkerRegistration<WorkerType> registration =
				new WorkerRegistration<>(taskExecutorGateway, newWorker, dataPort, hardwareDescription);

			log.info("Registering TaskManager with ResourceID {} ({}) at ResourceManager", taskExecutorResourceId, taskExecutorAddress);
			taskExecutors.put(taskExecutorResourceId, registration);

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

			return new TaskExecutorRegistrationSuccess(
				registration.getInstanceID(),//注册的从节点taskExecutor的ID
				resourceId,//resourceManager的资源id
				resourceManagerConfiguration.getHeartbeatInterval().toMilliseconds(),
				clusterInformation);//集群信息
		}
	}

	private void registerSlotAndTaskExecutorMetrics() {
		jobManagerMetricGroup.gauge(
			MetricNames.TASK_SLOTS_AVAILABLE,
			() -> (long) slotManager.getNumberFreeSlots());
		jobManagerMetricGroup.gauge(
			MetricNames.TASK_SLOTS_TOTAL,
			() -> (long) slotManager.getNumberRegisteredSlots());
		jobManagerMetricGroup.gauge(
			MetricNames.NUM_REGISTERED_TASK_MANAGERS,
			() -> (long) taskExecutors.size());
	}

	private void clearStateInternal() {
		jobManagerRegistrations.clear();
		jmResourceIdRegistrations.clear();
		taskExecutors.clear();

		try {
			jobLeaderIdService.clear();
		} catch (Exception e) {
			onFatalError(new ResourceManagerException("Could not properly clear the job leader id service.", e));
		}
		clearStateFuture = clearStateAsync();
	}

	/**
	 * This method should be called by the framework once it detects that a currently registered
	 * job manager has failed.
	 * 让jobmaster与resource manager断开连接
	 * @param jobId identifying the job whose leader shall be disconnected.
	 * @param cause The exception which cause the JobManager failed.
	 * 原因:有可能是jobmaster与resource manager无法心跳
	 * 或者jobmaster的leader被替换了,需要重新注册该job
	 */
	protected void closeJobManagerConnection(JobID jobId, Exception cause) {
		JobManagerRegistration jobManagerRegistration = jobManagerRegistrations.remove(jobId);//内存先删除该任务

		if (jobManagerRegistration != null) {
			final ResourceID jobManagerResourceId = jobManagerRegistration.getJobManagerResourceID();
			final JobMasterGateway jobMasterGateway = jobManagerRegistration.getJobManagerGateway();
			final JobMasterId jobMasterId = jobManagerRegistration.getJobMasterId();

			//让该job master与resource manager断开连接
			log.info("Disconnect job manager {}@{} for job {} from the resource manager.",
				jobMasterId,
				jobMasterGateway.getAddress(),
				jobId);

			jobManagerHeartbeatManager.unmonitorTarget(jobManagerResourceId);

			jmResourceIdRegistrations.remove(jobManagerResourceId);

			// tell the job manager about the disconnect
			jobMasterGateway.disconnectResourceManager(getFencingToken(), cause);//让job master关闭与resource manager的连接
		} else {
			log.debug("There was no registered job manager for job {}.", jobId);
		}
	}

	/**
	 * This method should be called by the framework once it detects that a currently registered
	 * task executor has failed.
	 *
	 * @param resourceID Id of the TaskManager that has failed.
	 * @param cause The exception which cause the TaskManager failed.
	 * 向TaskManager的网关发送信息，通知与该TaskManager失去连接,即连接超时
	 */
	protected void closeTaskManagerConnection(final ResourceID resourceID, final Exception cause) {
		taskManagerHeartbeatManager.unmonitorTarget(resourceID);

		WorkerRegistration<WorkerType> workerRegistration = taskExecutors.remove(resourceID);

		if (workerRegistration != null) {
			log.info("Closing TaskExecutor connection {} because: {}", resourceID, cause.getMessage());

			// TODO :: suggest failed task executor to stop itself
			slotManager.unregisterTaskManager(workerRegistration.getInstanceID());

			workerRegistration.getTaskExecutorGateway().disconnectResourceManager(cause);
		} else {
			log.debug(
				"No open TaskExecutor connection {}. Ignoring close TaskExecutor connection. Closing reason was: {}",
				resourceID,
				cause.getMessage());
		}
	}

	protected void removeJob(JobID jobId) {
		try {
			jobLeaderIdService.removeJob(jobId);//删除对该job的同步leader的关注
		} catch (Exception e) {
			log.warn("Could not properly remove the job {} from the job leader id service.", jobId, e);
		}

		if (jobManagerRegistrations.containsKey(jobId)) {
			disconnectJobManager(jobId, new Exception("Job " + jobId + "was removed"));
		}
	}

	//当job的leader被替换的时候,调用该函数,提供jobid 以及 老的leader
	protected void jobLeaderLostLeadership(JobID jobId, JobMasterId oldJobMasterId) {
		if (jobManagerRegistrations.containsKey(jobId)) {
			JobManagerRegistration jobManagerRegistration = jobManagerRegistrations.get(jobId);

			if (Objects.equals(jobManagerRegistration.getJobMasterId(), oldJobMasterId)) {
				disconnectJobManager(jobId, new Exception("Job leader lost leadership."));
			} else {
				log.debug("Discarding job leader lost leadership, because a new job leader was found for job {}. ", jobId);//说明job新的leader已经有了
			}
		} else {
			log.debug("Discard job leader lost leadership for outdated leader {} for job {}.", oldJobMasterId, jobId);//说明没有该job的信息
		}
	}

	protected void releaseResource(InstanceID instanceId, Exception cause) {
		WorkerType worker = null;

		// TODO: Improve performance by having an index on the instanceId
		for (Map.Entry<ResourceID, WorkerRegistration<WorkerType>> entry : taskExecutors.entrySet()) {
			if (entry.getValue().getInstanceID().equals(instanceId)) {
				worker = entry.getValue().getWorker();
				break;
			}
		}

		if (worker != null) {
			if (stopWorker(worker)) {
				closeTaskManagerConnection(worker.getResourceID(), cause);
			} else {
				log.debug("Worker {} could not be stopped.", worker.getResourceID());
			}
		} else {
			// unregister in order to clean up potential left over state
			slotManager.unregisterTaskManager(instanceId);
		}
	}

	// ------------------------------------------------------------------------
	//  Info messaging 通知所有节点，resource manager要发出去的信息
	// ------------------------------------------------------------------------

	public void sendInfoMessage(final String message) {
		getRpcService().execute(new Runnable() {
			@Override
			public void run() {
				InfoMessage infoMessage = new InfoMessage(message);
				for (InfoMessageListenerRpcGateway listenerRpcGateway : infoMessageListeners.values()) {
					listenerRpcGateway
						.notifyInfoMessage(infoMessage);
				}
			}
		});
	}

	// ------------------------------------------------------------------------
	//  Error Handling
	// ------------------------------------------------------------------------

	/**
	 * Notifies the ResourceManager that a fatal error has occurred and it cannot proceed.
	 *
	 * @param t The exception describing the fatal error
	 */
	protected void onFatalError(Throwable t) {
		try {
			log.error("Fatal error occurred in ResourceManager.", t);
		} catch (Throwable ignored) {}

		// The fatal error handler implementation should make sure that this call is non-blocking
		fatalErrorHandler.onFatalError(t);
	}

	// ------------------------------------------------------------------------
	//  Leader Contender
	// ------------------------------------------------------------------------

	/**
	 * Callback method when current resourceManager is granted leadership.
	 * 当该节点成为leader时，回调该方法
	 * @param newLeaderSessionID unique leadershipID
	 */
	@Override
	public void grantLeadership(final UUID newLeaderSessionID) {
		final CompletableFuture<Boolean> acceptLeadershipFuture = clearStateFuture
			.thenComposeAsync((ignored) -> tryAcceptLeadership(newLeaderSessionID), getUnfencedMainThreadExecutor());

		final CompletableFuture<Void> confirmationFuture = acceptLeadershipFuture.thenAcceptAsync(
			(acceptLeadership) -> {
				if (acceptLeadership) {//boolean类型
					// confirming the leader session ID might be blocking,
					leaderElectionService.confirmLeaderSessionID(newLeaderSessionID);//明确leader是谁
				}
			},
			getRpcService().getExecutor());

		confirmationFuture.whenComplete(
			(Void ignored, Throwable throwable) -> {
				if (throwable != null) {
					onFatalError(ExceptionUtils.stripCompletionException(throwable));
				}
			});
	}

	private CompletableFuture<Boolean> tryAcceptLeadership(final UUID newLeaderSessionID) {
		if (leaderElectionService.hasLeadership(newLeaderSessionID)) {//确定确实是选举服务产生的uuid
			final ResourceManagerId newResourceManagerId = ResourceManagerId.fromUuid(newLeaderSessionID);

			log.info("ResourceManager {} was granted leadership with fencing token {}", getAddress(), newResourceManagerId);

			// clear the state if we've been the leader before
			if (getFencingToken() != null) {
				clearStateInternal();
			}

			setFencingToken(newResourceManagerId);

			slotManager.start(getFencingToken(), getMainThreadExecutor(), new ResourceActionsImpl());

			return prepareLeadershipAsync().thenApply(ignored -> true);
		} else {
			return CompletableFuture.completedFuture(false);
		}
	}

	/**
	 * Callback method when current resourceManager loses leadership.
	 * 当resourceManager不再是leader的时候,调用该方法
	 */
	@Override
	public void revokeLeadership() {
		runAsyncWithoutFencing(
			() -> {
				log.info("ResourceManager {} was revoked leadership. Clearing fencing token.", getAddress());

				clearStateInternal();

				setFencingToken(null);

				slotManager.suspend();
			});
	}

	/**
	 * Handles error occurring in the leader election service.
	 *
	 * @param exception Exception being thrown in the leader election service
	 */
	@Override
	public void handleError(final Exception exception) {
		onFatalError(new ResourceManagerException("Received an error from the LeaderElectionService.", exception));
	}

	// ------------------------------------------------------------------------
	//  Framework specific behavior
	// ------------------------------------------------------------------------

	/**
	 * Initializes the framework specific components.
	 *
	 * @throws ResourceManagerException which occurs during initialization and causes the resource manager to fail.
	 */
	protected abstract void initialize() throws ResourceManagerException;

	/**
	 * This method can be overridden to add a (non-blocking) initialization routine to the
	 * ResourceManager that will be called when leadership is granted but before leadership is
	 * confirmed.
	 *
	 * @return Returns a {@code CompletableFuture} that completes when the computation is finished.
	 */
	protected CompletableFuture<Void> prepareLeadershipAsync() {
		return CompletableFuture.completedFuture(null);
	}

	/**
	 * This method can be overridden to add a (non-blocking) state clearing routine to the
	 * ResourceManager that will be called when leadership is revoked.
	 *
	 * @return Returns a {@code CompletableFuture} that completes when the state clearing routine
	 * is finished.
	 */
	protected CompletableFuture<Void> clearStateAsync() {
		return CompletableFuture.completedFuture(null);
	}

	/**
	 * The framework specific code to deregister the application. This should report the
	 * application's final status and shut down the resource manager cleanly.
	 *
	 * <p>This method also needs to make sure all pending containers that are not registered
	 * yet are returned.
	 *
	 * @param finalStatus The application status to report.
	 * @param optionalDiagnostics A diagnostics message or {@code null}.
	 * @throws ResourceManagerException if the application could not be shut down.
	 */
	protected abstract void internalDeregisterApplication(
		ApplicationStatus finalStatus,
		@Nullable String optionalDiagnostics) throws ResourceManagerException;

	/**
	 * Allocates a resource using the resource profile.
	 *
	 * @param resourceProfile The resource description
	 */
	@VisibleForTesting
	public abstract void startNewWorker(ResourceProfile resourceProfile);

	/**
	 * Callback when a worker was started.当worker已经被启动后,会被回调
	 * @param resourceID The worker resource id 返回resourceID
	 */
	protected abstract WorkerType workerStarted(ResourceID resourceID);

	/**
	 * Stops the given worker.
	 *
	 * @param worker The worker.
	 * @return True if the worker was stopped, otherwise false
	 */
	public abstract boolean stopWorker(WorkerType worker);

	// ------------------------------------------------------------------------
	//  Static utility classes
	// ------------------------------------------------------------------------
    //如何真正的申请资源和释放yarn的资源
	private class ResourceActionsImpl implements ResourceActions {

		@Override
		public void releaseResource(InstanceID instanceId, Exception cause) {
			validateRunsInMainThread();

			ResourceManager.this.releaseResource(instanceId, cause);
		}

		@Override
		public void allocateResource(ResourceProfile resourceProfile) throws ResourceManagerException {
			validateRunsInMainThread();
			startNewWorker(resourceProfile);
		}

		@Override
		public void notifyAllocationFailure(JobID jobId, AllocationID allocationId, Exception cause) {
			validateRunsInMainThread();

			JobManagerRegistration jobManagerRegistration = jobManagerRegistrations.get(jobId);
			if (jobManagerRegistration != null) {
				jobManagerRegistration.getJobManagerGateway().notifyAllocationFailure(allocationId, cause);
			}
		}
	}

	private class JobLeaderIdActionsImpl implements JobLeaderIdActions {

		//说明该job的leader被替换了
		@Override
		public void jobLeaderLostLeadership(final JobID jobId, final JobMasterId oldJobMasterId) {
			runAsync(new Runnable() {
				@Override
				public void run() {
					ResourceManager.this.jobLeaderLostLeadership(jobId, oldJobMasterId);
				}
			});
		}

		//说明该job的leader在规定时间内没有同步到
		@Override
		public void notifyJobTimeout(final JobID jobId, final UUID timeoutId) {
			runAsync(new Runnable() {
				@Override
				public void run() {
					if (jobLeaderIdService.isValidTimeout(jobId, timeoutId)) {
						removeJob(jobId);
					}
				}
			});
		}

		@Override
		public void handleError(Throwable error) {
			onFatalError(error);
		}
	}

	private class TaskManagerHeartbeatListener implements HeartbeatListener<SlotReport, Void> {

		//说明该task manager失去心跳了
		@Override
		public void notifyHeartbeatTimeout(final ResourceID resourceID) {
			runAsync(new Runnable() {
				@Override
				public void run() {
					log.info("The heartbeat of TaskManager with id {} timed out.", resourceID);

					closeTaskManagerConnection(
							resourceID,
							new TimeoutException("The heartbeat of TaskManager with id " + resourceID + "  timed out."));
				}
			});
		}

		//taskmanager不仅只要他活着，还要处理该节点上的slot报告信息。
		//参数taskmanager对应的资源、以及上报的信息
		@Override
		public void reportPayload(final ResourceID resourceID, final SlotReport slotReport) {
			runAsync(new Runnable() {
				@Override
				public void run() {
					log.debug("Received new slot report from TaskManager {}.", resourceID);

					final WorkerRegistration<WorkerType> workerRegistration = taskExecutors.get(resourceID);

					if (workerRegistration == null) {
						log.debug("Received slot report from TaskManager {} which is no longer registered.", resourceID);
					} else {
						InstanceID instanceId = workerRegistration.getInstanceID();

						slotManager.reportSlotStatus(instanceId, slotReport);
					}
				}
			});
		}

		//不需要发送给taskResource内容
		@Override
		public CompletableFuture<Void> retrievePayload(ResourceID resourceID) {
			return CompletableFuture.completedFuture(null);
		}
	}

	private class JobManagerHeartbeatListener implements HeartbeatListener<Void, Void> {

		@Override
		public void notifyHeartbeatTimeout(final ResourceID resourceID) {//说明该job失去了心跳
			runAsync(new Runnable() {
				@Override
				public void run() {
					log.info("The heartbeat of JobManager with id {} timed out.", resourceID);

					if (jmResourceIdRegistrations.containsKey(resourceID)) {
						JobManagerRegistration jobManagerRegistration = jmResourceIdRegistrations.get(resourceID);//在内存中对应的job master的信息

						if (jobManagerRegistration != null) {
							closeJobManagerConnection(jobManagerRegistration.getJobID(),new TimeoutException("The heartbeat of JobManager with id " + resourceID + " timed out."));
						}
					}
				}
			});
		}

		//jobmanager不需要处理客户端发来的信息，只知道他活着就可以。
		@Override
		public void reportPayload(ResourceID resourceID, Void payload) {
			// nothing to do since there is no payload
		}

		//不需要发送给taskResource内容
		@Override
		public CompletableFuture<Void> retrievePayload(ResourceID resourceID) {
			return CompletableFuture.completedFuture(null);
		}
	}

	// ------------------------------------------------------------------------
	//  Resource Management
	// ------------------------------------------------------------------------

	protected int getNumberPendingSlotRequests() {
		return slotManager.getNumberPendingSlotRequests();
	}
}

