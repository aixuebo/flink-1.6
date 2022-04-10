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

package org.apache.flink.runtime.resourcemanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobmaster.JobMasterId;

import java.util.UUID;

/**
 * Interface for actions called by the {@link JobLeaderIdService}.
 * 当job leader发生变更时，如何真正处理该变更逻辑
 */
public interface JobLeaderIdActions {

	/**
	 * Callback when a monitored job leader lost its leadership.
	 *
	 * @param jobId identifying the job whose leader lost leadership
	 * @param oldJobMasterId of the job manager which lost leadership 原masterId,即该masterid已经不再是leader了
	 * jobLeader超时没有心跳,则调该方法,提示哪个job,哪个uuid没有心跳了
	 * 当job更新了leader的时候，发生回调，提示哪个job的leader丢失了，正在产生新的leader过程中，传入已丢失的leader是谁，即老leader是谁
	 */
	void jobLeaderLostLeadership(JobID jobId, JobMasterId oldJobMasterId);

	/**
	 * Notify a job timeout. The job is identified by the given JobID. In order to check
	 * for the validity of the timeout the timeout id of the triggered timeout is provided.
	 *
	 * @param jobId JobID which identifies the timed out job
	 * @param timeoutId Id of the calling timeout to differentiate valid from invalid timeouts
	 * 在设置的超时时间内，依然不知道leader是谁，因此通知回调函数处理不知道leader是谁逻辑，即传入哪个job不知道leader，以及对应的超时任务id
	 */
	void notifyJobTimeout(JobID jobId, UUID timeoutId);

	/**
	 * Callback to report occurring errors.
	 *
	 * @param error which has occurred
	 * 当程序出错时,回调该方法
	 */
	void handleError(Throwable error);
}
