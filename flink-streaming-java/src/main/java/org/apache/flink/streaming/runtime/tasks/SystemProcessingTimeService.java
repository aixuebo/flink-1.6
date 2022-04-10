/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Delayed;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link ProcessingTimeService} which assigns as current processing time the result of calling
 * {@link System#currentTimeMillis()} and registers timers using a {@link ScheduledThreadPoolExecutor}.
 * 时间服务:计算当前处理时间戳、定时执行任务的能力
 */
public class SystemProcessingTimeService extends ProcessingTimeService {

	private static final Logger LOG = LoggerFactory.getLogger(SystemProcessingTimeService.class);

	private static final int STATUS_ALIVE = 0;//活着
	private static final int STATUS_QUIESCED = 1;//静止休眠状态
	private static final int STATUS_SHUTDOWN = 2;//shutdown

	// ------------------------------------------------------------------------

	/** The containing task that owns this time service provider. */
	private final AsyncExceptionHandler task;//如何处理其他线程产生的异常

	/** The lock that timers acquire upon triggering. */
	private final Object checkpointLock;

	/** The executor service that schedules and calls the triggers of this task.
	 * 定时调度器,用于执行定时调度回调事件
	 **/
	private final ScheduledThreadPoolExecutor timerService;//调度器

	private final AtomicInteger status;//对应枚举值状态

	public SystemProcessingTimeService(AsyncExceptionHandler failureHandler, Object checkpointLock) {
		this(failureHandler, checkpointLock, null);
	}

	public SystemProcessingTimeService(
			AsyncExceptionHandler task,//出现异常的时候如何处理
			Object checkpointLock,
			ThreadFactory threadFactory) {//线程工厂

		this.task = checkNotNull(task);
		this.checkpointLock = checkNotNull(checkpointLock);

		this.status = new AtomicInteger(STATUS_ALIVE);//设置默认的服务状态

		if (threadFactory == null) {
			this.timerService = new ScheduledThreadPoolExecutor(1);
		} else {
			this.timerService = new ScheduledThreadPoolExecutor(1, threadFactory);
		}

		// tasks should be removed if the future is canceled
		this.timerService.setRemoveOnCancelPolicy(true);

		// make sure shutdown removes all pending tasks
		this.timerService.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
		this.timerService.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
	}

	//返回当前时间戳,表示处理数据的当前时间
	@Override
	public long getCurrentProcessingTime() {
		return System.currentTimeMillis();
	}

	/**
	 * Registers a task to be executed no sooner than time {@code timestamp}, but without strong
	 * guarantees of order.
	 *
	 * @param timestamp Time when the task is to be enabled (in processing time) 当处理时间戳是该时间点时,执行target子任务
	 * @param target    The task to be executed
	 * @return The future that represents the scheduled task. This always returns some future,
	 *         even if the timer was shut down
	 * 注册一个延期执行的任务
	 */
	@Override
	public ScheduledFuture<?> registerTimer(long timestamp, ProcessingTimeCallback target) {

		// delay the firing of the timer by 1 ms to align the semantics with watermark. A watermark
		// T says we won't see elements in the future with a timestamp smaller or equal to T.
		// With processing time, we therefore need to delay firing the timer by one ms.
		long delay = Math.max(timestamp - getCurrentProcessingTime(), 0) + 1;//计算延迟多久后,要执行target

		// we directly try to register the timer and only react to the status on exception
		// that way we save unnecessary volatile accesses for each timer
		try {
			//定时器调度
			return timerService.schedule(
					new TriggerTask(status, task, checkpointLock, target, timestamp), delay, TimeUnit.MILLISECONDS);//调度,延迟delay后,执行TriggerTask
		}
		catch (RejectedExecutionException e) {
			final int status = this.status.get();
			if (status == STATUS_QUIESCED) {
				return new NeverCompleteFuture(delay);//服务暂时静止中,无法提供服务
			}
			else if (status == STATUS_SHUTDOWN) {
				throw new IllegalStateException("Timer service is shut down");//不再提供服务
			}
			else {
				// something else happened, so propagate the exception
				throw e;
			}
		}
	}

	//当前时间戳+initialDelay后,开始周期性的执行callback任务
	@Override
	public ScheduledFuture<?> scheduleAtFixedRate(ProcessingTimeCallback callback, long initialDelay, long period) {
		long nextTimestamp = getCurrentProcessingTime() + initialDelay;

		// we directly try to register the timer and only react to the status on exception
		// that way we save unnecessary volatile accesses for each timer
		try {
			return timerService.scheduleAtFixedRate(
				new RepeatedTriggerTask(status, task, checkpointLock, callback, nextTimestamp, period),//执行该任务
				initialDelay,//初始时,延迟多久
				period,//周期性的执行该任务
				TimeUnit.MILLISECONDS);
		} catch (RejectedExecutionException e) {
			final int status = this.status.get();
			if (status == STATUS_QUIESCED) {
				return new NeverCompleteFuture(initialDelay);
			}
			else if (status == STATUS_SHUTDOWN) {
				throw new IllegalStateException("Timer service is shut down");
			}
			else {
				// something else happened, so propagate the exception
				throw e;
			}
		}
	}

	/**
	 * @return {@code true} is the status of the service
	 * is {@link #STATUS_ALIVE}, {@code false} otherwise.
	 */
	@VisibleForTesting
	boolean isAlive() {
		return status.get() == STATUS_ALIVE;
	}

	@Override
	public boolean isTerminated() {
		return status.get() == STATUS_SHUTDOWN;
	}

	//设置休眠状态
	@Override
	public void quiesce() throws InterruptedException {
		if (status.compareAndSet(STATUS_ALIVE, STATUS_QUIESCED)) {
			timerService.shutdown();
		}
	}

	@Override
	public void awaitPendingAfterQuiesce() throws InterruptedException {
		if (!timerService.isTerminated()) {
			Preconditions.checkState(timerService.isTerminating() || timerService.isShutdown());

			// await forever (almost)
			timerService.awaitTermination(365L, TimeUnit.DAYS);
		}
	}

	@Override
	public void shutdownService() {
		if (status.compareAndSet(STATUS_ALIVE, STATUS_SHUTDOWN) ||
				status.compareAndSet(STATUS_QUIESCED, STATUS_SHUTDOWN)) {
			timerService.shutdownNow();
		}
	}

	//等候一阵子后,进行终止服务
	@Override
	public boolean shutdownAndAwaitPending(long time, TimeUnit timeUnit) throws InterruptedException {
		shutdownService();
		return timerService.awaitTermination(time, timeUnit);
	}

	@Override
	public boolean shutdownServiceUninterruptible(long timeoutMs) {

		final Deadline deadline = Deadline.fromNow(Duration.ofMillis(timeoutMs));

		boolean shutdownComplete = false;
		boolean receivedInterrupt = false;

		do {
			try {
				// wait for a reasonable time for all pending timer threads to finish
				shutdownComplete = shutdownAndAwaitPending(deadline.timeLeft().toMillis(), TimeUnit.MILLISECONDS);
			} catch (InterruptedException iex) {
				receivedInterrupt = true;
				LOG.trace("Intercepted attempt to interrupt timer service shutdown.", iex);
			}
		} while (deadline.hasTimeLeft() && !shutdownComplete);

		if (receivedInterrupt) {
			Thread.currentThread().interrupt();
		}

		return shutdownComplete;
	}

	// safety net to destroy the thread pool
	@Override
	protected void finalize() throws Throwable {
		super.finalize();
		timerService.shutdownNow();
	}

	//队列中有多少个任务在调度中
	@VisibleForTesting
	int getNumTasksScheduled() {
		BlockingQueue<?> queue = timerService.getQueue();
		if (queue == null) {
			return 0;
		} else {
			return queue.size();
		}
	}

	// ------------------------------------------------------------------------

	/**
	 * Internal task that is invoked by the timer service and triggers the target.
	 * 回调函数---时间到期后,定时器线程执行该任务
	 */
	private static final class TriggerTask implements Runnable {

		private final AtomicInteger serviceStatus;
		private final Object lock;
		private final ProcessingTimeCallback target;//需要执行的任务
		private final long timestamp;//期待到这个时间点,则执行target任务
		private final AsyncExceptionHandler exceptionHandler;//出现异常时,如何处理

		private TriggerTask(
				final AtomicInteger serviceStatus,
				final AsyncExceptionHandler exceptionHandler,
				final Object lock,
				final ProcessingTimeCallback target,
				final long timestamp) {

			this.serviceStatus = Preconditions.checkNotNull(serviceStatus);
			this.exceptionHandler = Preconditions.checkNotNull(exceptionHandler);
			this.lock = Preconditions.checkNotNull(lock);
			this.target = Preconditions.checkNotNull(target);
			this.timestamp = timestamp;
		}

		@Override
		public void run() {
			synchronized (lock) {
				try {
					if (serviceStatus.get() == STATUS_ALIVE) {//状态是活着时,执行触发函数
						target.onProcessingTime(timestamp);//传入期待的执行时间点
					}
				} catch (Throwable t) {
					TimerException asyncException = new TimerException(t);
					exceptionHandler.handleAsyncException("Caught exception while processing timer.", asyncException);//执行异常函数
				}
			}
		}
	}

	/**
	 * Internal task which is repeatedly called by the processing time service.
	 * 周期性的重复执行该任务
	 */
	private static final class RepeatedTriggerTask implements Runnable {

		private final AtomicInteger serviceStatus;//服务的状态,可能服务已经停止对外提供了
		private final Object lock;
		private final ProcessingTimeCallback target;//任务本身
		private final long period;//执行周期
		private final AsyncExceptionHandler exceptionHandler;//出现异常时,如何处理

		private long nextTimestamp;//下一次执行的时间戳

		private RepeatedTriggerTask(
				final AtomicInteger serviceStatus,
				final AsyncExceptionHandler exceptionHandler,
				final Object lock,
				final ProcessingTimeCallback target,
				final long nextTimestamp,
				final long period) {

			this.serviceStatus = Preconditions.checkNotNull(serviceStatus);
			this.lock = Preconditions.checkNotNull(lock);
			this.target = Preconditions.checkNotNull(target);
			this.period = period;
			this.exceptionHandler = Preconditions.checkNotNull(exceptionHandler);

			this.nextTimestamp = nextTimestamp;
		}

		@Override
		public void run() {
			synchronized (lock) {
				try {
					if (serviceStatus.get() == STATUS_ALIVE) {
						target.onProcessingTime(nextTimestamp);
					}

					nextTimestamp += period;//下一次调用run方法的时候,nextTimestamp要产生新的,因此本轮计算好
				} catch (Throwable t) {
					TimerException asyncException = new TimerException(t);
					exceptionHandler.handleAsyncException("Caught exception while processing repeated timer task.", asyncException);//处理异常任务
				}
			}
		}
	}

	// ------------------------------------------------------------------------
    //当服务暂时不对外提供时,如何返回值
	private static final class NeverCompleteFuture implements ScheduledFuture<Object> {

		private final Object lock = new Object();

		private final long delayMillis;

		private volatile boolean canceled;

		private NeverCompleteFuture(long delayMillis) {
			this.delayMillis = delayMillis;
		}

		@Override
		public long getDelay(@Nonnull TimeUnit unit) {
			return unit.convert(delayMillis, TimeUnit.MILLISECONDS);
		}

		@Override
		public int compareTo(@Nonnull Delayed o) {
			long otherMillis = o.getDelay(TimeUnit.MILLISECONDS);
			return Long.compare(this.delayMillis, otherMillis);
		}

		@Override
		public boolean cancel(boolean mayInterruptIfRunning) {
			synchronized (lock) {
				canceled = true;
				lock.notifyAll();
			}
			return true;
		}

		@Override
		public boolean isCancelled() {
			return canceled;
		}

		@Override
		public boolean isDone() {
			return false;
		}

		@Override
		public Object get() throws InterruptedException {
			synchronized (lock) {
				while (!canceled) {
					lock.wait();
				}
			}
			throw new CancellationException();
		}

		@Override
		public Object get(long timeout, @Nonnull TimeUnit unit) throws InterruptedException, TimeoutException {
			synchronized (lock) {
				while (!canceled) {
					unit.timedWait(lock, timeout);
				}

				if (canceled) {
					throw new CancellationException();
				} else {
					throw new TimeoutException();
				}
			}
		}
	}
}
