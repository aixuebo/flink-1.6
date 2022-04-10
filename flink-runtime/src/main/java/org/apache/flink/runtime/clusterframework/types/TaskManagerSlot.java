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

package org.apache.flink.runtime.clusterframework.types;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.runtime.resourcemanager.slotmanager.PendingSlotRequest;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A TaskManagerSlot represents a slot located in a TaskManager. It has a unique identification and
 * resource profile associated.
 * 站在slot维度下,列举该slot的属性  即描述了一个slot的资源情况、分配给哪个job用了
 */
public class TaskManagerSlot {

	/** The unique identification of this slot. */
	private final SlotID slotId;

	/** The resource profile of this slot. */
	private final ResourceProfile resourceProfile;//所占用资源

	/** Gateway to the TaskExecutor which owns the slot. */
	private final TaskExecutorConnection taskManagerConnection;//归属于哪个taskManager

	/** Allocation id for which this slot has been allocated. */
	private AllocationID allocationId;//目前该slot分配给哪个资源id --- 即分配给哪个jobID、以及该job需要的资源情况

	/** Allocation id for which this slot has been allocated. */
	@Nullable
	private JobID jobId;//归属于哪个job

	/** Assigned slot request if there is currently an ongoing request. */
	private PendingSlotRequest assignedSlotRequest;//正在分配给哪个请求

	private State state;

	public TaskManagerSlot(
			SlotID slotId,
			ResourceProfile resourceProfile,
			TaskExecutorConnection taskManagerConnection) {
		this.slotId = checkNotNull(slotId);
		this.resourceProfile = checkNotNull(resourceProfile);
		this.taskManagerConnection = checkNotNull(taskManagerConnection);

		this.state = State.FREE;
		this.allocationId = null;
		this.assignedSlotRequest = null;
	}

	public State getState() {
		return state;
	}

	public SlotID getSlotId() {
		return slotId;
	}

	public ResourceProfile getResourceProfile() {
		return resourceProfile;
	}

	public TaskExecutorConnection getTaskManagerConnection() {
		return taskManagerConnection;
	}

	public AllocationID getAllocationId() {
		return allocationId;
	}

	@Nullable
	public JobID getJobId() {
		return jobId;
	}

	public PendingSlotRequest getAssignedSlotRequest() {
		return assignedSlotRequest;
	}

	public InstanceID getInstanceId() {
		return taskManagerConnection.getInstanceID();
	}

	//释放该资源slot,即不给任何job使用
	public void freeSlot() {
		Preconditions.checkState(state == State.ALLOCATED, "Slot must be allocated before freeing it.");

		state = State.FREE;
		allocationId = null;
		jobId = null;
	}


	public void clearPendingSlotRequest() {
		Preconditions.checkState(state == State.PENDING, "No slot request to clear.");
		state = State.FREE;
		assignedSlotRequest = null;
	}

	//正在分配给某一个等待的请求
	public void assignPendingSlotRequest(PendingSlotRequest pendingSlotRequest) {
		Preconditions.checkState(state == State.FREE, "Slot must be free to be assigned a slot request.");

		state = State.PENDING;
		assignedSlotRequest = Preconditions.checkNotNull(pendingSlotRequest);
	}

	//完成分配
	public void completeAllocation(AllocationID allocationId, JobID jobId) {
		Preconditions.checkNotNull(allocationId, "Allocation id must not be null.");
		Preconditions.checkNotNull(jobId, "Job id must not be null.");
		Preconditions.checkState(state == State.PENDING, "In order to complete an allocation, the slot has to be allocated.");
		Preconditions.checkState(Objects.equals(allocationId, assignedSlotRequest.getAllocationId()), "Mismatch between allocation id of the pending slot request.");

		state = State.ALLOCATED;
		this.allocationId = allocationId;
		this.jobId = jobId;
		assignedSlotRequest = null;
	}

	//该slot分配给哪个job的哪个allocationId  --- 说明资源分配了该job的其他请求,即可以重复利用资源
	public void updateAllocation(AllocationID allocationId, JobID jobId) {
		Preconditions.checkState(state == State.FREE, "The slot has to be free in order to set an allocation id.");

		state = State.ALLOCATED;
		this.allocationId = Preconditions.checkNotNull(allocationId);
		this.jobId = Preconditions.checkNotNull(jobId);
	}

	/**
	 * Check whether required resource profile can be matched by this slot.
	 *
	 * @param required The required resource profile
	 * @return true if requirement can be matched
	 * 是否满足参数的资源需求
	 */
	public boolean isMatchingRequirement(ResourceProfile required) {
		return resourceProfile.isMatching(required);
	}

	/**
	 * State of the {@link TaskManagerSlot}.
	 */
	public enum State {
		FREE,//空闲
		PENDING,//正在分配中
		ALLOCATED//已经分配给了job+allocationId
	}
}
