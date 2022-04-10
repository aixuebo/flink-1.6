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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobmaster.JMTMRegistrationSuccess;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.jobmaster.JobMasterId;

/**
 * Listener for the {@link JobLeaderService}. The listener is notified whenever a job manager
 * gained leadership for a registered job and the service could establish a connection to it.
 * Furthermore, the listener is notified when a job manager loses leadership for a job. In case
 * of an error, the {@link #handleError(Throwable)}} is called.
 * 当job leader发生变化时,触发回调函数
 */
public interface JobLeaderListener {

	/**
	 * Callback if a job manager has gained leadership for the job identified by the job id and a
	 * connection could be established to this job manager.
	 *
	 * @param jobId identifying the job for which the job manager has gained leadership
	 * @param jobManagerGateway to the job leader,job对应的网关服务引用
	 * @param registrationMessage containing further registration information,jobmanager返回的response
	 * 当task节点成功与jobmanager连接后,回调该函数
	 */
	void jobManagerGainedLeadership(JobID jobId, JobMasterGateway jobManagerGateway, JMTMRegistrationSuccess registrationMessage);

	/**
	 * Callback if the job leader for the job with the given job id lost its leadership.
	 *
	 * @param jobId identifying the job whose leader has lost leadership
	 * @param jobMasterId old JobMasterId
	 * 当监听zookeeper时,发现job的jobMasterId 不再是leader,则回调该函数
	 */
	void jobManagerLostLeadership(JobID jobId, JobMasterId jobMasterId);

	/**
	 * Callback for errors which might occur in the {@link JobLeaderService}.
	 *
	 * @param throwable cause
	 * 说明当监听job的leader过程,出现异常的时候,调用该回调方法
	 */
	void handleError(Throwable throwable);
}
