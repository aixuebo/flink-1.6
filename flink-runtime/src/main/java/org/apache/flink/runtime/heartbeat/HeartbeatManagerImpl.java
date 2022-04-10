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
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;

import javax.annotation.concurrent.ThreadSafe;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Heartbeat manager implementation. The heartbeat manager maintains a map of heartbeat monitors
 * and resource IDs. Each monitor will be updated when a new heartbeat of the associated machine has
 * been received. If the monitor detects that a heartbeat has timed out, it will notify the
 * {@link HeartbeatListener} about it. A heartbeat times out iff no heartbeat signal has been
 * received within a given timeout interval.
 *
 * @param <I> Type of the incoming heartbeat payload
 * @param <O> Type of the outgoing heartbeat payload
 */
@ThreadSafe
public class HeartbeatManagerImpl<I, O> implements HeartbeatManager<I, O> {

	/** Heartbeat timeout interval in milli seconds.心跳的超时时间 */
	private final long heartbeatTimeoutIntervalMs;//在超时时限内,必须接到发过来的心跳信息

	/** Resource ID which is used to mark one own's heartbeat signals.拥有该心跳服务的对象  比如resource manager需要开启一个心跳服务，因此该资源id就是 resource manager */
	private final ResourceID ownResourceID;//比如taskExecutor

	/** Heartbeat listener with which the heartbeat manager has been associated. 当产生心跳的时候，如何处理 */
	private final HeartbeatListener<I, O> heartbeatListener;

	/** Executor service used to run heartbeat timeout notifications. */
	private final ScheduledExecutor scheduledExecutor;

	protected final Logger log;

	/** Map containing the heartbeat monitors associated with the respective resource ID. 心跳服务需要关注的资源集合，即resource manager需要与哪些节点交互*/
	private final ConcurrentHashMap<ResourceID, HeartbeatManagerImpl.HeartbeatMonitor<O>> heartbeatTargets;

	/** Execution context used to run future callbacks. */
	private final Executor executor;

	/** Running state of the heartbeat manager. */
	protected volatile boolean stopped;

	public HeartbeatManagerImpl(
			long heartbeatTimeoutIntervalMs,
			ResourceID ownResourceID,
			HeartbeatListener<I, O> heartbeatListener,
			Executor executor,
			ScheduledExecutor scheduledExecutor,
			Logger log) {
		Preconditions.checkArgument(heartbeatTimeoutIntervalMs > 0L, "The heartbeat timeout has to be larger than 0.");

		this.heartbeatTimeoutIntervalMs = heartbeatTimeoutIntervalMs;
		this.ownResourceID = Preconditions.checkNotNull(ownResourceID);
		this.heartbeatListener = Preconditions.checkNotNull(heartbeatListener, "heartbeatListener");
		this.scheduledExecutor = Preconditions.checkNotNull(scheduledExecutor);
		this.log = Preconditions.checkNotNull(log);
		this.executor = Preconditions.checkNotNull(executor);
		this.heartbeatTargets = new ConcurrentHashMap<>(16);

		stopped = false;
	}

	//----------------------------------------------------------------------------------------------
	// Getters
	//----------------------------------------------------------------------------------------------

	ResourceID getOwnResourceID() {
		return ownResourceID;
	}

	Executor getExecutor() {
		return executor;
	}

	HeartbeatListener<I, O> getHeartbeatListener() {
		return heartbeatListener;
	}

	Collection<HeartbeatMonitor<O>> getHeartbeatTargets() {
		return heartbeatTargets.values();
	}

	//----------------------------------------------------------------------------------------------
	// HeartbeatManager methods
	//----------------------------------------------------------------------------------------------

	/**
	 * 监听某一个资源的请求,比如taskExecutor要监听所有jobManager,因此每一个jobManager都会调用该方法
	 * @param resourceID Resource ID identifying the heartbeat target,此时场景下,id是jobManager的唯一id,即task节点要关注所有的jobManager心跳
	 * @param heartbeatTarget Interface to send heartbeat requests and responses to the heartbeat
	 *                        target
	 */
	@Override
	public void monitorTarget(ResourceID resourceID, HeartbeatTarget<O> heartbeatTarget) {
		if (!stopped) {
			if (heartbeatTargets.containsKey(resourceID)) { //task节点监控所有jobManager,因此资源id是jobManager
				log.debug("The target with resource ID {} is already been monitored.", resourceID);
			} else {
				HeartbeatManagerImpl.HeartbeatMonitor<O> heartbeatMonitor = new HeartbeatManagerImpl.HeartbeatMonitor<>(
					resourceID,
					heartbeatTarget,
					scheduledExecutor,
					heartbeatListener,
					heartbeatTimeoutIntervalMs);

				heartbeatTargets.put(
					resourceID,
					heartbeatMonitor);

				// check if we have stopped in the meantime (concurrent stop operation)
				if (stopped) {
					heartbeatMonitor.cancel();//取消该心跳任务的监听

					heartbeatTargets.remove(resourceID);
				}
			}
		}
	}

	//不再监听该资源的心跳请求
	@Override
	public void unmonitorTarget(ResourceID resourceID) {
		if (!stopped) {
			HeartbeatManagerImpl.HeartbeatMonitor<O> heartbeatMonitor = heartbeatTargets.remove(resourceID);

			if (heartbeatMonitor != null) {//取消该心跳任务的监听
				heartbeatMonitor.cancel();
			}
		}
	}

	@Override
	public void stop() {
		stopped = true;

		for (HeartbeatManagerImpl.HeartbeatMonitor<O> heartbeatMonitor : heartbeatTargets.values()) {//取消所有的心跳任务的监听
			heartbeatMonitor.cancel();
		}

		heartbeatTargets.clear();
	}

	//获取收到该资源上一次请求心跳的时间戳
	@Override
	public long getLastHeartbeatFrom(ResourceID resourceId) {
		HeartbeatMonitor<O> heartbeatMonitor = heartbeatTargets.get(resourceId);

		if (heartbeatMonitor != null) {
			return heartbeatMonitor.getLastHeartbeat();
		} else {
			return -1L;
		}
	}

	//----------------------------------------------------------------------------------------------
	// HeartbeatTarget methods
	//----------------------------------------------------------------------------------------------

	//接收到客户端发过来的心跳
	//参数是哪个客户端发来的心跳 I = input
	@Override
	public void receiveHeartbeat(ResourceID heartbeatOrigin, I heartbeatPayload) {//谁发过来的心跳，以及心跳内容
		if (!stopped) {
			log.debug("Received heartbeat from {}.", heartbeatOrigin);
			reportHeartbeat(heartbeatOrigin);//接收到该心跳--更细上一次心跳时间

			if (heartbeatPayload != null) {
				heartbeatListener.reportPayload(heartbeatOrigin, heartbeatPayload);//收到具体的心跳内容
			}
		}
	}


	//这个逻辑与HeartbeatTarget的requestHeartbeat不太相同。注意一下。

	//接受心跳请求 && response回复信息给对方
	@Override
	public void requestHeartbeat(final ResourceID requestOrigin, I heartbeatPayload) {
		if (!stopped) {
			log.debug("Received heartbeat request from {}.", requestOrigin);

			final HeartbeatTarget<O> heartbeatTarget = reportHeartbeat(requestOrigin);//更新资源的心跳,返回response对象 --- 注意HeartbeatTarget泛型是<O>

			if (heartbeatTarget != null) {
				if (heartbeatPayload != null) {
					heartbeatListener.reportPayload(requestOrigin, heartbeatPayload);//先接受信息---注意heartbeatListener的泛型是<I,O>
				}

				CompletableFuture<O> futurePayload = heartbeatListener.retrievePayload(requestOrigin);//要返回resource manager的内容,计算待response的内容

				if (futurePayload != null) {//说明有response返回
					CompletableFuture<Void> sendHeartbeatFuture = futurePayload.thenAcceptAsync(
						//retrievedPayload 即是等待返回的response内容
						retrievedPayload ->	heartbeatTarget.receiveHeartbeat(getOwnResourceID(), retrievedPayload),//告诉resource manager自己是谁
						executor);

					sendHeartbeatFuture.exceptionally((Throwable failure) -> {
							log.warn("Could not send heartbeat to target with id {}.", requestOrigin, failure);

							return null;
						});
				} else {
					heartbeatTarget.receiveHeartbeat(ownResourceID, null);//告诉客户端自己是谁
				}
			}
		}
	}

	//仅仅做一次心跳更新,返回需要输出的response对象
	HeartbeatTarget<O> reportHeartbeat(ResourceID resourceID) {
		if (heartbeatTargets.containsKey(resourceID)) {
			HeartbeatManagerImpl.HeartbeatMonitor<O> heartbeatMonitor = heartbeatTargets.get(resourceID);
			heartbeatMonitor.reportHeartbeat();//接收到该心跳--更细上一次心跳时间

			return heartbeatMonitor.getHeartbeatTarget();//返回与该资源交互的对象
		} else {
			return null;
		}
	}

	//----------------------------------------------------------------------------------------------
	// Utility classes
	//----------------------------------------------------------------------------------------------

	/**
	 * Heartbeat monitor which manages the heartbeat state of the associated heartbeat target. The
	 * monitor notifies the {@link HeartbeatListener} whenever it has not seen a heartbeat signal
	 * in the specified heartbeat timeout interval. Each heartbeat signal resets this timer.
	 *
	 * @param <O> Type of the payload being sent to the associated heartbeat target
	 * 需要监听的对象
	 */
	static class HeartbeatMonitor<O> implements Runnable {

		/** Resource ID of the monitored heartbeat target. 被监听的资源id ，比如resource manager需要关注所有与他相连接的job manager，因为该id就是某一个job manager*/
		private final ResourceID resourceID;

		/** Associated heartbeat target.需要与该job manager相互交互内容 */
		private final HeartbeatTarget<O> heartbeatTarget;

		private final ScheduledExecutor scheduledExecutor;

		/** Listener which is notified about heartbeat timeouts. */
		private final HeartbeatListener<?, ?> heartbeatListener;

		/** Maximum heartbeat timeout interval. 心跳超时时间*/
		private final long heartbeatTimeoutIntervalMs;

		private volatile ScheduledFuture<?> futureTimeout;//超时返回值

		private final AtomicReference<State> state = new AtomicReference<>(State.RUNNING);

		private volatile long lastHeartbeat;//上一次接收到心跳的时间

		HeartbeatMonitor(
			ResourceID resourceID,
			HeartbeatTarget<O> heartbeatTarget,
			ScheduledExecutor scheduledExecutor,
			HeartbeatListener<?, O> heartbeatListener,
			long heartbeatTimeoutIntervalMs) {

			this.resourceID = Preconditions.checkNotNull(resourceID);
			this.heartbeatTarget = Preconditions.checkNotNull(heartbeatTarget);
			this.scheduledExecutor = Preconditions.checkNotNull(scheduledExecutor);
			this.heartbeatListener = Preconditions.checkNotNull(heartbeatListener);

			Preconditions.checkArgument(heartbeatTimeoutIntervalMs > 0L, "The heartbeat timeout interval has to be larger than 0.");
			this.heartbeatTimeoutIntervalMs = heartbeatTimeoutIntervalMs;

			lastHeartbeat = 0L;

			resetHeartbeatTimeout(heartbeatTimeoutIntervalMs);
		}

		HeartbeatTarget<O> getHeartbeatTarget() {
			return heartbeatTarget;
		}

		ResourceID getHeartbeatTargetId() {
			return resourceID;
		}

		public long getLastHeartbeat() {
			return lastHeartbeat;
		}

		//说明此时收到了心跳,因此设置收到心跳的时间
		void reportHeartbeat() {
			lastHeartbeat = System.currentTimeMillis();
			resetHeartbeatTimeout(heartbeatTimeoutIntervalMs);//重新开启一个等待超时心跳的任务
		}

		//重新设置心跳，将上一个心跳任务删除，重新发起一个心跳任务
		void resetHeartbeatTimeout(long heartbeatTimeout) {
			if (state.get() == State.RUNNING) {
				cancelTimeout();//删除上一个心跳超时

				//设置一个超时时间调度器，如果在超时时间后，则调run方法,返回futureTimeout对象。
				futureTimeout = scheduledExecutor.schedule(this, heartbeatTimeout, TimeUnit.MILLISECONDS);//重新产生一个心跳超时任务

				// Double check for concurrent accesses (e.g. a firing of the scheduled future)
				if (state.get() != State.RUNNING) {//任务已经不是运行中了,双重保障该超时任务失效。
					cancelTimeout();
				}
			}
		}

		//设置状态为cancel
		void cancel() {
			// we can only cancel if we are in state running
			if (state.compareAndSet(State.RUNNING, State.CANCELED)) {
				cancelTimeout();
			}
		}

		//取消超时任务
		private void cancelTimeout() {
			if (futureTimeout != null) {
				futureTimeout.cancel(true);
			}
		}

		public boolean isCanceled() {
			return state.get() == State.CANCELED;
		}

		//说明超时时间已经到了,因此状态变成超时,并且通知监听器
		@Override
		public void run() {
			// The heartbeat has timed out if we're in state running
			if (state.compareAndSet(State.RUNNING, State.TIMEOUT)) {
				heartbeatListener.notifyHeartbeatTimeout(resourceID);//说明该任务已经超时了
			}
		}

		private enum State {
			RUNNING,//运行中
			TIMEOUT,//已超时
			CANCELED//取消
		}
	}
}
