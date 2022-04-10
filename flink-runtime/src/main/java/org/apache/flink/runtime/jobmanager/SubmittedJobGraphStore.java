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

package org.apache.flink.runtime.jobmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.JobGraph;

import javax.annotation.Nullable;

import java.util.Collection;

/**
 * {@link SubmittedJobGraph} instances for recovery.
 * 如何实例化job全局的元数据信息，比如用zookeeper实现
 */
public interface SubmittedJobGraphStore {

	/**
	 * Starts the {@link SubmittedJobGraphStore} service.
	 * 打开一个存储job的服务。
	 * 参数表示:当job发生删除、新增时，如何做通知
	 */
	void start(SubmittedJobGraphListener jobGraphListener) throws Exception;

	/**
	 * Stops the {@link SubmittedJobGraphStore} service.
	 */
	void stop() throws Exception;

	/**
	 * Returns the {@link SubmittedJobGraph} with the given {@link JobID} or
	 * {@code null} if no job was registered.
	 * 恢复还原给定jobId
	 */
	@Nullable
	SubmittedJobGraph recoverJobGraph(JobID jobId) throws Exception;

	/**
	 * Adds the {@link SubmittedJobGraph} instance.
	 *
	 * <p>If a job graph with the same {@link JobID} exists, it is replaced.
	 * 新增/更新一个job
	 */
	void putJobGraph(SubmittedJobGraph jobGraph) throws Exception;

	/**
	 * Removes the {@link SubmittedJobGraph} with the given {@link JobID} if it exists.
	 * 删除一个job
	 */
	void removeJobGraph(JobID jobId) throws Exception;

	/**
	 * Releases the locks on the specified {@link JobGraph}.
	 *
	 * Releasing the locks allows that another instance can delete the job from
	 * the {@link SubmittedJobGraphStore}.
	 *
	 * @param jobId specifying the job to release the locks for
	 * @throws Exception if the locks cannot be released
	 * 释放该job的锁对象
	 */
	void releaseJobGraph(JobID jobId) throws Exception;

	/**
	 * Get all job ids of submitted job graphs to the submitted job graph store.
	 *
	 * @return Collection of submitted job ids
	 * @throws Exception if the operation fails
	 * 获取全部的job信息集合
	 */
	Collection<JobID> getJobIds() throws Exception;

	/**
	 * A listener for {@link SubmittedJobGraph} instances. This is used to react to races between
	 * multiple running {@link SubmittedJobGraphStore} instances (on multiple job managers).
	 * 任务的提交/删除,子类用于接到通知时，该如何处理
	 */
	interface SubmittedJobGraphListener {

		/**
		 * Callback for {@link SubmittedJobGraph} instances added by a different {@link
		 * SubmittedJobGraphStore} instance.
		 *
		 * <p><strong>Important:</strong> It is possible to get false positives and be notified
		 * about a job graph, which was added by this instance.
		 *
		 * @param jobId The {@link JobID} of the added job graph
		 * 第一次新增一个job时触发
		 */
		void onAddedJobGraph(JobID jobId);

		/**
		 * Callback for {@link SubmittedJobGraph} instances removed by a different {@link
		 * SubmittedJobGraphStore} instance.
		 *
		 * @param jobId The {@link JobID} of the removed job graph
		 * 删除一个job时触发
		 */
		void onRemovedJobGraph(JobID jobId);
	}
}
