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

package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.util.ConfigurationException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.concurrent.duration.Duration;

/**
 * Configuration for the {@link SlotManager}.
 * 配置三种超时时间
 */
public class SlotManagerConfiguration {

	private static final Logger LOGGER = LoggerFactory.getLogger(SlotManagerConfiguration.class);

	private final Time taskManagerRequestTimeout;//调用task manager的rpc超时时间 ,即与task manager交互的超时时间
	private final Time slotRequestTimeout;//jobmanager请求的资源，长时间未分配slot,因此超时
	private final Time taskManagerTimeout;//某一个taskmanager长时间没有分配任务，则说明该节点有超时

	public SlotManagerConfiguration(
			Time taskManagerRequestTimeout,
			Time slotRequestTimeout,
			Time taskManagerTimeout) {
		this.taskManagerRequestTimeout = Preconditions.checkNotNull(taskManagerRequestTimeout);
		this.slotRequestTimeout = Preconditions.checkNotNull(slotRequestTimeout);
		this.taskManagerTimeout = Preconditions.checkNotNull(taskManagerTimeout);
	}

	public Time getTaskManagerRequestTimeout() {
		return taskManagerRequestTimeout;
	}

	public Time getSlotRequestTimeout() {
		return slotRequestTimeout;
	}

	public Time getTaskManagerTimeout() {
		return taskManagerTimeout;
	}

	public static SlotManagerConfiguration fromConfiguration(Configuration configuration) throws ConfigurationException {
		final String strTimeout = configuration.getString(AkkaOptions.ASK_TIMEOUT);
		final Time rpcTimeout;

		try {
			rpcTimeout = Time.milliseconds(Duration.apply(strTimeout).toMillis());
		} catch (NumberFormatException e) {
			throw new ConfigurationException("Could not parse the resource manager's timeout " +
				"value " + AkkaOptions.ASK_TIMEOUT + '.', e);
		}

		final Time slotRequestTimeout = getSlotRequestTimeout(configuration);
		final Time taskManagerTimeout = Time.milliseconds(
				configuration.getLong(ResourceManagerOptions.TASK_MANAGER_TIMEOUT));

		return new SlotManagerConfiguration(rpcTimeout, slotRequestTimeout, taskManagerTimeout);
	}

	private static Time getSlotRequestTimeout(final Configuration configuration) {
		final long slotRequestTimeoutMs;
		if (configuration.contains(ResourceManagerOptions.SLOT_REQUEST_TIMEOUT)) {
			LOGGER.warn("Config key {} is deprecated; use {} instead.",
				ResourceManagerOptions.SLOT_REQUEST_TIMEOUT,
				JobManagerOptions.SLOT_REQUEST_TIMEOUT);
			slotRequestTimeoutMs = configuration.getLong(ResourceManagerOptions.SLOT_REQUEST_TIMEOUT);
		} else {
			slotRequestTimeoutMs = configuration.getLong(JobManagerOptions.SLOT_REQUEST_TIMEOUT);
		}
		return Time.milliseconds(slotRequestTimeoutMs);
	}
}
