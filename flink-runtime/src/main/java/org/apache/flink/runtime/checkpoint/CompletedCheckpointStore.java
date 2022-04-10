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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.runtime.jobgraph.JobStatus;

import java.util.List;

/**
 * A bounded LIFO-queue of {@link CompletedCheckpoint} instances.
 * 先进先出的方式,保存所有已完成的checkpoint对象的存储容器
 */
public interface CompletedCheckpointStore {

	/**
	 * Recover available {@link CompletedCheckpoint} instances.
	 *
	 * <p>After a call to this method, {@link #getLatestCheckpoint()} returns the latest
	 * available checkpoint.
	 *
	 * 重新恢复,仅保留zookeeper上的最新的信息,抛弃掉非zookeeper上所有的checkpoint文件 --- 将CompletedCheckpoint对象保存到内存
	 */
	void recover() throws Exception;

	/**
	 * Adds a {@link CompletedCheckpoint} instance to the list of completed checkpoints.
	 *
	 * <p>Only a bounded number of checkpoints is kept. When exceeding the maximum number of
	 * retained checkpoints, the oldest one will be discarded.
	 * 添加一个完成的checkpoint文件
	 */
	void addCheckpoint(CompletedCheckpoint checkpoint) throws Exception;

	/**
	 * Returns the latest {@link CompletedCheckpoint} instance or <code>null</code> if none was
	 * added.
	 * 返回最后一个添加的checkpoint文件
	 */
	CompletedCheckpoint getLatestCheckpoint() throws Exception;

	/**
	 * Shuts down the store.
	 *
	 * <p>The job status is forwarded and used to decide whether state should
	 * actually be discarded or kept.
	 *
	 * @param jobStatus Job state on shut down
	 */
	void shutdown(JobStatus jobStatus) throws Exception;

	/**
	 * Returns all {@link CompletedCheckpoint} instances.
	 *
	 * <p>Returns an empty list if no checkpoint has been added yet.
	 * 返回所有checkpoint文件
	 */
	List<CompletedCheckpoint> getAllCheckpoints() throws Exception;

	/**
	 * Returns the current number of retained checkpoints.
	 * 存储已经真实存放了多少个checkpoint文件
	 */
	int getNumberOfRetainedCheckpoints();

	/**
	 * Returns the max number of retained checkpoints.
	 * 总容器 最多允许存储多少个checkpoint文件
	 */
	int getMaxNumberOfRetainedCheckpoints();

	/**
	 * This method returns whether the completed checkpoint store requires checkpoints to be
	 * externalized. Externalized checkpoints have their meta data persisted, which the checkpoint
	 * store can exploit (for example by simply pointing the persisted metadata).
	 * 
	 * @return True, if the store requires that checkpoints are externalized before being added, false
	 *         if the store stores the metadata itself.
	 *
	 *  是否是外部存储,比如zookeeper就是外部存储,因此返回true
	 */
	boolean requiresExternalizedCheckpoints();
}
