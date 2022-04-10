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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.TransientBlobKey;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.PartitionInfo;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.StackTraceSampleResponse;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcTimeout;
import org.apache.flink.runtime.taskmanager.Task;

import java.util.concurrent.CompletableFuture;

/**
 * {@link TaskExecutor} RPC gateway interface.
 * task节点对外提供的接口能力
 */
public interface TaskExecutorGateway extends RpcGateway {

	/**
	 * Requests a slot from the TaskManager.
	 * 在task manager上的一个slot去执行任务
	 * 执行的任务是 jobid 在某一次请求id(allocationId)申请的资源。
	 *
	 * 与job manager通信，通过jobmanager的请求id--allocationId，就知道需要执行哪部分任务了。
	 * @param slotId slot id for the request 该task manager上的slot id，使用该slot去执行任务
	 * @param jobId for which to request a slot
	 * @param allocationId id for the request //slot唯一ID
	 * @param targetAddress to which to offer the requested slots 要知道job manager的地址，方便与job manager通信
	 * @param resourceManagerId current leader id of the ResourceManager 知道ResourceManager的id
	 * @param timeout for the operation 连接到taskmanager的请求超时时间
	 * @return answer to the slot request
	 *
	 * 由resourceManager发起调用,从resourceManagerId上,收到一个slot请求,将该slot分配给job
	 */
	CompletableFuture<Acknowledge> requestSlot(
		SlotID slotId,
		JobID jobId,
		AllocationID allocationId,
		String targetAddress,
		ResourceManagerId resourceManagerId,
		@RpcTimeout Time timeout);

	CompletableFuture<StackTraceSampleResponse> requestStackTraceSample(
		ExecutionAttemptID executionAttemptId,
		int sampleId,
		int numSamples,
		Time delayBetweenSamples,
		int maxStackTraceDepth,
		@RpcTimeout Time timeout);

	/**
	 * Submit a {@link Task} to the {@link TaskExecutor}.
	 *
	 * @param tdd describing the task to submit
	 * @param jobMasterId identifying the submitting JobMaster
	 * @param timeout of the submit operation
	 * @return Future acknowledge of the successful operation
	 */
	CompletableFuture<Acknowledge> submitTask(
		TaskDeploymentDescriptor tdd,
		JobMasterId jobMasterId,
		@RpcTimeout Time timeout);

	/**
	 * Update the task where the given partitions can be found.
	 *
	 * @param executionAttemptID identifying the task
	 * @param partitionInfos telling where the partition can be retrieved from
	 * @param timeout for the update partitions operation
	 * @return Future acknowledge if the partitions have been successfully updated
	 */
	CompletableFuture<Acknowledge> updatePartitions(
		ExecutionAttemptID executionAttemptID,
		Iterable<PartitionInfo> partitionInfos,
		@RpcTimeout Time timeout);

	/**
	 * Fail all intermediate result partitions of the given task.
	 *
	 * @param executionAttemptID identifying the task
	 */
	void failPartition(ExecutionAttemptID executionAttemptID);

	/**
	 * Trigger the checkpoint for the given task. The checkpoint is identified by the checkpoint ID
	 * and the checkpoint timestamp.
	 *
	 * @param executionAttemptID identifying the task
	 * @param checkpointID unique id for the checkpoint
	 * @param checkpointTimestamp is the timestamp when the checkpoint has been initiated
	 * @param checkpointOptions for performing the checkpoint
	 * @return Future acknowledge if the checkpoint has been successfully triggered
	 */
	CompletableFuture<Acknowledge> triggerCheckpoint(ExecutionAttemptID executionAttemptID, long checkpointID, long checkpointTimestamp, CheckpointOptions checkpointOptions);

	/**
	 * Confirm a checkpoint for the given task. The checkpoint is identified by the checkpoint ID
	 * and the checkpoint timestamp.
	 *
	 * @param executionAttemptID identifying the task
	 * @param checkpointId unique id for the checkpoint
	 * @param checkpointTimestamp is the timestamp when the checkpoint has been initiated
	 * @return Future acknowledge if the checkpoint has been successfully confirmed
	 */
	CompletableFuture<Acknowledge> confirmCheckpoint(ExecutionAttemptID executionAttemptID, long checkpointId, long checkpointTimestamp);

	/**
	 * Stop the given task.
	 *
	 * @param executionAttemptID identifying the task
	 * @param timeout for the stop operation
	 * @return Future acknowledge if the task is successfully stopped
	 */
	CompletableFuture<Acknowledge> stopTask(ExecutionAttemptID executionAttemptID, @RpcTimeout Time timeout);

	/**
	 * Cancel the given task.
	 *
	 * @param executionAttemptID identifying the task
	 * @param timeout for the cancel operation
	 * @return Future acknowledge if the task is successfully canceled
	 */
	CompletableFuture<Acknowledge> cancelTask(ExecutionAttemptID executionAttemptID, @RpcTimeout Time timeout);

	/**
	 * Heartbeat request from the job manager.
	 *
	 * @param heartbeatOrigin unique id of the job manager 标识job manager
	 * 接受jobmanager心跳请求 && response回复信息给对方
	 */
	void heartbeatFromJobManager(ResourceID heartbeatOrigin);

	/**
	 * Heartbeat request from the resource manager.
	 *
	 * @param heartbeatOrigin unique id of the resource manager 标识resource manager
	 * 接受ResourceManager心跳请求 && response回复信息给对方
	 */
	void heartbeatFromResourceManager(ResourceID heartbeatOrigin);

	/**
	 * Disconnects the given JobManager from the TaskManager.
	 *
	 * @param jobId JobID for which the JobManager was the leader
	 * @param cause for the disconnection from the JobManager
	 */
	void disconnectJobManager(JobID jobId, Exception cause);

	/**
	 * Disconnects the ResourceManager from the TaskManager.
	 *
	 * @param cause for the disconnection from the ResourceManager
	 */
	void disconnectResourceManager(Exception cause);

	/**
	 * Frees the slot with the given allocation ID.
	 *
	 * @param allocationId identifying the slot to free
	 * @param cause of the freeing operation
	 * @param timeout for the operation
	 * @return Future acknowledge which is returned once the slot has been freed
	 */
	CompletableFuture<Acknowledge> freeSlot(
		final AllocationID allocationId,
		final Throwable cause,
		@RpcTimeout final Time timeout);

	/**
	 * Requests the file upload of the specified type to the cluster's {@link BlobServer}.
	 *
	 * @param fileType to upload
	 * @param timeout for the asynchronous operation
	 * @return Future which is completed with the {@link TransientBlobKey} of the uploaded file.
	 * resourceManager请求task网关,让task把相关日志上传到blob服务
	 * 将文件上传到blob服务器上,返回上传成功后的MD5
	 */
	CompletableFuture<TransientBlobKey> requestFileUpload(FileType fileType, @RpcTimeout Time timeout);
}
