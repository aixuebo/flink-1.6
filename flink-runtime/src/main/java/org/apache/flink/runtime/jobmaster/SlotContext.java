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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

/**
 * Interface for the context of a {@link LogicalSlot}. This context contains information
 * about the underlying allocated slot and how to communicate with the TaskManager on which
 * it was allocated.
 * 表示slot的上下文信息
 */
public interface SlotContext {
	/**
	 * Gets the id under which the slot has been allocated on the TaskManager. This id uniquely identifies the
	 * physical slot.
	 *
	 * @return The id under which the slot has been allocated on the TaskManager
	 * slot的唯一ID
	 */
	AllocationID getAllocationId();

	/**
	 * Gets the location info of the TaskManager that offers this slot.
	 *
	 * @return The location info of the TaskManager that offers this slot
	 * task节点信息
	 */
	TaskManagerLocation getTaskManagerLocation();

	/**
	 * Gets the number of the slot.
	 *
	 * @return The number of the slot on the TaskManager.
	 * slot在task节点的物理序号
	 */
	int getPhysicalSlotNumber();

	/**
	 * Gets the actor gateway that can be used to send messages to the TaskManager.
	 * <p>
	 * This method should be removed once the new interface-based RPC abstraction is in place
	 *
	 * @return The gateway that can be used to send messages to the TaskManager.
	 * task节点的网关
	 */
	TaskManagerGateway getTaskManagerGateway();
}
