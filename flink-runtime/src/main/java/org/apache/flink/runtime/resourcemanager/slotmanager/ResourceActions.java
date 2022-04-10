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
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.resourcemanager.exceptions.ResourceManagerException;

/**
 * Resource related actions which the {@link SlotManager} can perform.
 */
public interface ResourceActions {

	/**
	 * Releases the resource with the given instance id.
	 *
	 * @param instanceId identifying which resource to release
	 * @param cause why the resource is released
	 * 释放某个资源，比如当taskManager失联时,释放该taskManager,给出释放原因
	 */
	void releaseResource(InstanceID instanceId, Exception cause);

	/**
	 * Requests to allocate a resource with the given {@link ResourceProfile}.
	 *
	 * @param resourceProfile for the to be allocated resource 需要申请的资源
	 * @throws ResourceManagerException if the resource cannot be allocated
	 * //当taskmanager的资源不足时,申请新的资源
	 */
	void allocateResource(ResourceProfile resourceProfile) throws ResourceManagerException;

	/**
	 * Notifies that an allocation failure has occurred.
	 *
	 * @param jobId to which the allocation belonged 标识哪个job请求资源失败了
	 * @param allocationId identifying the failed allocation 请求的唯一id
	 * @param cause of the allocation failure 给出失败原因
	 * 发送一个通知,通知该job的某一个资源请求没有成功,给出原因
	 */
	void notifyAllocationFailure(JobID jobId, AllocationID allocationId, Exception cause);
}
