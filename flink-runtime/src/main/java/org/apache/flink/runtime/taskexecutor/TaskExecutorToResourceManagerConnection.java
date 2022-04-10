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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.instance.HardwareDescription;
import org.apache.flink.runtime.registration.RegisteredRpcConnection;
import org.apache.flink.runtime.registration.RegistrationConnectionListener;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.registration.RetryingRegistration;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.rpc.RpcService;

import org.slf4j.Logger;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The connection between a TaskExecutor and the ResourceManager.
 * 一个从节点如何去连接ResourceManager
 */
public class TaskExecutorToResourceManagerConnection
		extends RegisteredRpcConnection<ResourceManagerId, ResourceManagerGateway, TaskExecutorRegistrationSuccess> {

	private final RpcService rpcService;

	private final String taskManagerAddress;

	private final ResourceID taskManagerResourceId;

	private final int dataPort;

	private final HardwareDescription hardwareDescription;

	private final RegistrationConnectionListener<TaskExecutorToResourceManagerConnection, TaskExecutorRegistrationSuccess> registrationListener;

	public TaskExecutorToResourceManagerConnection(
			Logger log,
			RpcService rpcService,
			String taskManagerAddress,//从节点的地址
			ResourceID taskManagerResourceId,//从节点的uuid
			int dataPort,//从节点端口
			HardwareDescription hardwareDescription,//从节点资源描述
			String resourceManagerAddress,//ResourceManager的地址
			ResourceManagerId resourceManagerId,//ResourceManager的uuid
			Executor executor,//本地如何执行runnable任务
			RegistrationConnectionListener<TaskExecutorToResourceManagerConnection, TaskExecutorRegistrationSuccess> registrationListener) {

		super(log, resourceManagerAddress, resourceManagerId, executor);

		this.rpcService = checkNotNull(rpcService);
		this.taskManagerAddress = checkNotNull(taskManagerAddress);
		this.taskManagerResourceId = checkNotNull(taskManagerResourceId);
		this.dataPort = dataPort;
		this.hardwareDescription = checkNotNull(hardwareDescription);
		this.registrationListener = checkNotNull(registrationListener);
	}

	@Override
	protected RetryingRegistration<ResourceManagerId, ResourceManagerGateway, TaskExecutorRegistrationSuccess> generateRegistration() {
		return new TaskExecutorToResourceManagerConnection.ResourceManagerRegistration(
			log,
			rpcService,
			getTargetAddress(),
			getTargetLeaderId(),
			taskManagerAddress,
			taskManagerResourceId,
			dataPort,
			hardwareDescription);
	}

	//成功收到注册成功返回值
	@Override
	protected void onRegistrationSuccess(TaskExecutorRegistrationSuccess success) {
		log.info("Successful registration at resource manager {} under registration id {}.",
			getTargetAddress(), success.getRegistrationId());

		registrationListener.onRegistrationSuccess(this, success);
	}

	//注册失败后,收到返回值
	@Override
	protected void onRegistrationFailure(Throwable failure) {
		log.info("Failed to register at resource manager {}.", getTargetAddress(), failure);

		registrationListener.onRegistrationFailure(failure);
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	//真实的一个注册请求是如何完成的
	private static class ResourceManagerRegistration
			extends RetryingRegistration<ResourceManagerId, ResourceManagerGateway, TaskExecutorRegistrationSuccess> {

		//task从节点信息内容
		private final String taskExecutorAddress;

		private final ResourceID resourceID;

		private final int dataPort;

		private final HardwareDescription hardwareDescription;

		ResourceManagerRegistration(
				Logger log,
				RpcService rpcService,
				String targetAddress,//远程服务器地址
				ResourceManagerId resourceManagerId,//远程服务器交互对象
				String taskExecutorAddress,//从节点服务器地址
				ResourceID resourceID,//从节点uuid
				int dataPort,
				HardwareDescription hardwareDescription) {//从节点资源

			super(log, rpcService, "ResourceManager", ResourceManagerGateway.class, targetAddress, resourceManagerId);
			this.taskExecutorAddress = checkNotNull(taskExecutorAddress);
			this.resourceID = checkNotNull(resourceID);
			this.dataPort = dataPort;
			this.hardwareDescription = checkNotNull(hardwareDescription);
		}

		//真实的执行注册请求
		@Override
		protected CompletableFuture<RegistrationResponse> invokeRegistration(
				ResourceManagerGateway resourceManager, ResourceManagerId fencingToken, long timeoutMillis) throws Exception {

			Time timeout = Time.milliseconds(timeoutMillis);
			return resourceManager.registerTaskExecutor(
				taskExecutorAddress,
				resourceID,
				dataPort,
				hardwareDescription,
				timeout);
		}
	}
}
