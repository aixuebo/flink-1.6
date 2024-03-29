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

package org.apache.flink.runtime.taskexecutor.slot;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.AllocationID;

/**
 * Exception indicating that the given {@link TaskSlot} was not in state active.
 * slot已经不是活着的slot了
 */
public class SlotNotActiveException extends Exception {

	private static final long serialVersionUID = 4305837511564584L;

	public SlotNotActiveException(JobID jobId, AllocationID allocationId) {
		super("No active slot for job " + jobId + " with allocation id " + allocationId + '.');
	}
}
