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

/**
 * Internal task slot state
 */
enum TaskSlotState {
	ACTIVE, // Slot is in active use by a job manager responsible for a job 该slot已经分配给了job,并且分配了job manager
	ALLOCATED, // Slot has been allocated for a job but not yet given to a job manager 该slot已经分配给了job，但没有分配job manager
	RELEASING, // Slot is not empty but tasks are failed. Upon removal of all tasks, it will be released 该slot正在释放
	FREE // Slot is free 该slot是空闲的
}
