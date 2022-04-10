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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.resourcemanager.SlotRequest;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;

//因为jobManager发给resourceManager的请求不能立刻有资源,因此需要等待。
public class PendingSlotRequest {

	private final SlotRequest slotRequest;//缓存持有请求的资源

	@Nullable
	private CompletableFuture<Acknowledge> requestFuture;//返回请求的资源

	/** Timestamp when this pending slot request has been created. */
	private final long creationTimestamp;//请求什么时间点被等待的,有可能后续需要根据等待时间的前后顺序安排资源

	public PendingSlotRequest(SlotRequest slotRequest) {
		this.slotRequest = Preconditions.checkNotNull(slotRequest);
		creationTimestamp = System.currentTimeMillis();
	}

	// ------------------------------------------------------------------------

	public AllocationID getAllocationId() {
		return slotRequest.getAllocationId();
	}

	public ResourceProfile getResourceProfile() {
		return slotRequest.getResourceProfile();
	}

	public JobID getJobId() {
		return slotRequest.getJobId();
	}

	public String getTargetAddress() {
		return slotRequest.getTargetAddress();
	}

	public long getCreationTimestamp() {
		return creationTimestamp;
	}

	//true表示请求已经分配
	public boolean isAssigned() {
		return null != requestFuture;
	}

	public void setRequestFuture(@Nullable CompletableFuture<Acknowledge> requestFuture) {
		this.requestFuture = requestFuture;
	}

	@Nullable
	public CompletableFuture<Acknowledge> getRequestFuture() {
		return requestFuture;
	}
}
