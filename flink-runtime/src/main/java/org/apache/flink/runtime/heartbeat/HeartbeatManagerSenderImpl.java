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

package org.apache.flink.runtime.heartbeat;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;

import org.slf4j.Logger;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * {@link HeartbeatManager} implementation which regularly requests a heartbeat response from
 * its monitored {@link HeartbeatTarget}. The heartbeat period is configurable.
 *
 * @param <I> Type of the incoming heartbeat payload
 * @param <O> Type of the outgoing heartbeat payload
 *
 * 作为服务端，定期让客户端发送给自己心跳信息。
 * 比如resource manager要求定期让jobmanager、taskmanager给自己发送心跳
 */
public class HeartbeatManagerSenderImpl<I, O> extends HeartbeatManagerImpl<I, O> implements Runnable {

	private final ScheduledFuture<?> triggerFuture;//周期性调度任务--执行该类

	public HeartbeatManagerSenderImpl(
			long heartbeatPeriod,//发送心跳周期
			long heartbeatTimeout,//两个节点之间超时时间,超过该时间,则说明节点超时,确定丢失心跳
			ResourceID ownResourceID,
			HeartbeatListener<I, O> heartbeatListener,
			Executor executor,
			ScheduledExecutor scheduledExecutor,
			Logger log) {
		super(
			heartbeatTimeout,
			ownResourceID,
			heartbeatListener,
			executor,
			scheduledExecutor,
			log);

		triggerFuture = scheduledExecutor.scheduleAtFixedRate(this, 0L, heartbeatPeriod, TimeUnit.MILLISECONDS);
	}

	//周期性的执行该run方法
	@Override
	public void run() {
		if (!stopped) {
			log.debug("Trigger heartbeat request.");
			for (HeartbeatMonitor<O> heartbeatMonitor : getHeartbeatTargets()) {//与自己关联的jobmanager、taskmanager
				CompletableFuture<O> futurePayload = getHeartbeatListener().retrievePayload(heartbeatMonitor.getHeartbeatTargetId());//要发送客户端的内容
				final HeartbeatTarget<O> heartbeatTarget = heartbeatMonitor.getHeartbeatTarget();//返回response的内容

				if (futurePayload != null) {
					CompletableFuture<Void> requestHeartbeatFuture = futurePayload.thenAcceptAsync(
						//payload表示要发送的内容
						payload -> heartbeatTarget.requestHeartbeat(getOwnResourceID(), payload),//向客户端发送信息,要求客户端知道自己是谁、以及传递的信息内容
						getExecutor());

					requestHeartbeatFuture.exceptionally(
						(Throwable failure) -> {
							log.warn("Could not request the heartbeat from target {}.", heartbeatTarget, failure);

							return null;
						});
				} else {
					heartbeatTarget.requestHeartbeat(getOwnResourceID(), null);
				}
			}
		}
	}

	@Override
	public void stop() {
			triggerFuture.cancel(true);
			super.stop();
	}
}
