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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.taskmanager.TaskActions;

/**
 * Interface for notifications about consumable partitions.
 * 分区有数据写入,或者已经完成写入时,触发该通知,让下游开始调度
 */
public interface ResultPartitionConsumableNotifier {
	//哪个job的哪个partitionId已经有数据了
	void notifyPartitionConsumable(JobID jobId, ResultPartitionID partitionId, TaskActions taskActions);
}
