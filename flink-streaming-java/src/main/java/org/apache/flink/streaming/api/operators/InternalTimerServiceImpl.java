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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.CompatibilityResult;
import org.apache.flink.api.common.typeutils.CompatibilityUtil;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.InternalPriorityQueue;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link InternalTimerService} that stores timers on the Java heap.
 * 使用内存存储InternalTimerService
 * 基于内存不靠谱,所以后面内容会有快照存储的方式。
 *
 * 因为reduce是针对某一个KeyGroupRange的，因此InternalTimerServiceImpl也归属于某一个KeyGroupRange
 */
public class InternalTimerServiceImpl<K, N> implements InternalTimerService<N>, ProcessingTimeCallback {

	private final ProcessingTimeService processingTimeService;//提供调度定时任务的能力

	private final KeyContext keyContext;//如何获取key,因为InternalTimer是由时间戳+key+命名空间组成的

	/**
	 * Processing time timers that are currently in-flight.
	 * 处理队列
	 */
	private final KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<K, N>> processingTimeTimersQueue;

	/**
	 * Event time timers that are currently in-flight.
	 */
	private final KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<K, N>> eventTimeTimersQueue;

	/**
	 * Information concerning the local key-group range.
	 * key的范围
	 */
	private final KeyGroupRange localKeyGroupRange;

	private final int localKeyGroupRangeStartIdx;//本地key最小的值

	/**
	 * The local event time, as denoted by the last received
	 * {@link org.apache.flink.streaming.api.watermark.Watermark Watermark}.
	 */
	private long currentWatermark = Long.MIN_VALUE;

	/**
	 * The one and only Future (if any) registered to execute the
	 * next {@link Triggerable} action, when its (processing) time arrives.
	 * 下一次触发的调度任务返回值,该对象可以获取调用后的结果，或者取消调用
	 * */
	private ScheduledFuture<?> nextTimer;

	// Variables to be set when the service is started.

	private TypeSerializer<K> keySerializer;

	private TypeSerializer<N> namespaceSerializer;

	private Triggerable<K, N> triggerTarget;//时间触发时的具体逻辑

	private volatile boolean isInitialized;

	private TypeSerializer<K> keyDeserializer;

	private TypeSerializer<N> namespaceDeserializer;

	/** The restored timers snapshot, if any. */
	private InternalTimersSnapshot<K, N> restoredTimersSnapshot;

	InternalTimerServiceImpl(
		KeyGroupRange localKeyGroupRange,
		KeyContext keyContext,
		ProcessingTimeService processingTimeService,
		KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<K, N>> processingTimeTimersQueue,
		KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<K, N>> eventTimeTimersQueue) {

		this.keyContext = checkNotNull(keyContext);
		this.processingTimeService = checkNotNull(processingTimeService);
		this.localKeyGroupRange = checkNotNull(localKeyGroupRange);
		this.processingTimeTimersQueue = checkNotNull(processingTimeTimersQueue);
		this.eventTimeTimersQueue = checkNotNull(eventTimeTimersQueue);

		// find the starting index of the local key-group range
		int startIdx = Integer.MAX_VALUE;
		for (Integer keyGroupIdx : localKeyGroupRange) {
			startIdx = Math.min(keyGroupIdx, startIdx);
		}
		this.localKeyGroupRangeStartIdx = startIdx;
	}

	/**
	 * Starts the local {@link InternalTimerServiceImpl} by:
	 * <ol>
	 *     <li>Setting the {@code keySerialized} and {@code namespaceSerializer} for the timers it will contain.</li>
	 *     <li>Setting the {@code triggerTarget} which contains the action to be performed when a timer fires.</li>
	 *     <li>Re-registering timers that were retrieved after recovering from a node failure, if any.</li>
	 * </ol>
	 * This method can be called multiple times, as long as it is called with the same serializers.
	 */
	public void startTimerService(
			TypeSerializer<K> keySerializer,
			TypeSerializer<N> namespaceSerializer,
			Triggerable<K, N> triggerTarget) {

		if (!isInitialized) {

			if (keySerializer == null || namespaceSerializer == null) {
				throw new IllegalArgumentException("The TimersService serializers cannot be null.");
			}

			if (this.keySerializer != null || this.namespaceSerializer != null || this.triggerTarget != null) {
				throw new IllegalStateException("The TimerService has already been initialized.");
			}

			// the following is the case where we restore
			if (restoredTimersSnapshot != null) {
				CompatibilityResult<K> keySerializerCompatibility = CompatibilityUtil.resolveCompatibilityResult(
					this.keyDeserializer,
					null,
					restoredTimersSnapshot.getKeySerializerConfigSnapshot(),
					keySerializer);

				CompatibilityResult<N> namespaceSerializerCompatibility = CompatibilityUtil.resolveCompatibilityResult(
					this.namespaceDeserializer,
					null,
					restoredTimersSnapshot.getNamespaceSerializerConfigSnapshot(),
					namespaceSerializer);

				if (keySerializerCompatibility.isRequiresMigration() || namespaceSerializerCompatibility.isRequiresMigration()) {
					throw new IllegalStateException("Tried to initialize restored TimerService " +
						"with incompatible serializers than those used to snapshot its state.");
				}
			}

			this.keySerializer = keySerializer;
			this.namespaceSerializer = namespaceSerializer;
			this.keyDeserializer = null;
			this.namespaceDeserializer = null;

			this.triggerTarget = Preconditions.checkNotNull(triggerTarget);

			// re-register the restored timers (if any)
			final InternalTimer<K, N> headTimer = processingTimeTimersQueue.peek();
			if (headTimer != null) {
				nextTimer = processingTimeService.registerTimer(headTimer.getTimestamp(), this);
			}
			this.isInitialized = true;
		} else {
			if (!(this.keySerializer.equals(keySerializer) && this.namespaceSerializer.equals(namespaceSerializer))) {
				throw new IllegalArgumentException("Already initialized Timer Service " +
					"tried to be initialized with different key and namespace serializers.");
			}
		}
	}

	@Override
	public long currentProcessingTime() {
		return processingTimeService.getCurrentProcessingTime();
	}

	@Override
	public long currentWatermark() {
		return currentWatermark;
	}

	@Override
	public void registerProcessingTimeTimer(N namespace, long time) {
		InternalTimer<K, N> oldHead = processingTimeTimersQueue.peek();
		if (processingTimeTimersQueue.add(new TimerHeapInternalTimer<>(time, (K) keyContext.getCurrentKey(), namespace))) {//追加成功
			long nextTriggerTime = oldHead != null ? oldHead.getTimestamp() : Long.MAX_VALUE;//下一次被调用的最早的时间戳
			// check if we need to re-schedule our timer to earlier
			if (time < nextTriggerTime) { //说明本次添加的，应该更早被调用
				if (nextTimer != null) {
					nextTimer.cancel(false);//取消调度任务
				}
				nextTimer = processingTimeService.registerTimer(time, this);//设置最新的调度任务
			}
		}
	}

	@Override
	public void registerEventTimeTimer(N namespace, long time) {
		eventTimeTimersQueue.add(new TimerHeapInternalTimer<>(time, (K) keyContext.getCurrentKey(), namespace));//追加队列
	}

	//从队列删除
	@Override
	public void deleteProcessingTimeTimer(N namespace, long time) {
		processingTimeTimersQueue.remove(new TimerHeapInternalTimer<>(time, (K) keyContext.getCurrentKey(), namespace));
	}

	@Override
	public void deleteEventTimeTimer(N namespace, long time) {
		eventTimeTimersQueue.remove(new TimerHeapInternalTimer<>(time, (K) keyContext.getCurrentKey(), namespace));
	}

	//统一处理所有的回调函数
	@Override
	public void onProcessingTime(long time) throws Exception {
		// null out the timer in case the Triggerable calls registerProcessingTimeTimer()
		// inside the callback.
		nextTimer = null;

		InternalTimer<K, N> timer;

		while ((timer = processingTimeTimersQueue.peek()) != null && timer.getTimestamp() <= time) {
			processingTimeTimersQueue.poll();
			keyContext.setCurrentKey(timer.getKey());
			triggerTarget.onProcessingTime(timer);
		}

		//*** 很重要的优化: 确保只有一个时间点在时间调度里
		if (timer != null && nextTimer == null) {
			nextTimer = processingTimeService.registerTimer(timer.getTimestamp(), this);
		}
	}

	public void advanceWatermark(long time) throws Exception {
		currentWatermark = time;

		InternalTimer<K, N> timer;

		while ((timer = eventTimeTimersQueue.peek()) != null && timer.getTimestamp() <= time) {
			eventTimeTimersQueue.poll();
			keyContext.setCurrentKey(timer.getKey());
			triggerTarget.onEventTime(timer);
		}
	}

	/**
	 * Snapshots the timers (both processing and event time ones) for a given {@code keyGroupIdx}.
	 *
	 * @param keyGroupIdx the id of the key-group to be put in the snapshot.
	 * @return a snapshot containing the timers for the given key-group, and the serializers for them
	 * 针对某一个分区内的数据做快照
	 */
	public InternalTimersSnapshot<K, N> snapshotTimersForKeyGroup(int keyGroupIdx) {
		return new InternalTimersSnapshot<>(
			keySerializer,
			keySerializer.snapshotConfiguration(),
			namespaceSerializer,
			namespaceSerializer.snapshotConfiguration(),
			eventTimeTimersQueue.getSubsetForKeyGroup(keyGroupIdx),
			processingTimeTimersQueue.getSubsetForKeyGroup(keyGroupIdx));
	}

	/**
	 * Restore the timers (both processing and event time ones) for a given {@code keyGroupIdx}.
	 *
	 * @param restoredSnapshot the restored snapshot containing the key-group's timers,
	 *                       and the serializers that were used to write them
	 * @param keyGroupIdx the id of the key-group to be put in the snapshot.
	 * 针对某一个分区内的数据快照还原
	 */
	@SuppressWarnings("unchecked")
	public void restoreTimersForKeyGroup(InternalTimersSnapshot<?, ?> restoredSnapshot, int keyGroupIdx) {
		this.restoredTimersSnapshot = (InternalTimersSnapshot<K, N>) restoredSnapshot;

		if (areSnapshotSerializersIncompatible(restoredSnapshot)) {
			throw new IllegalArgumentException("Tried to restore timers " +
				"for the same service with different serializers.");
		}

		this.keyDeserializer = restoredTimersSnapshot.getKeySerializer();
		this.namespaceDeserializer = restoredTimersSnapshot.getNamespaceSerializer();

		checkArgument(localKeyGroupRange.contains(keyGroupIdx),
			"Key Group " + keyGroupIdx + " does not belong to the local range.");

		// restore the event time timers
		eventTimeTimersQueue.addAll(restoredTimersSnapshot.getEventTimeTimers());

		// restore the processing time timers
		processingTimeTimersQueue.addAll(restoredTimersSnapshot.getProcessingTimeTimers());
	}

	//事件数量
	@VisibleForTesting
	public int numProcessingTimeTimers() {
		return this.processingTimeTimersQueue.size();
	}

	@VisibleForTesting
	public int numEventTimeTimers() {
		return this.eventTimeTimersQueue.size();
	}

	//空间下注册的time数量
	@VisibleForTesting
	public int numProcessingTimeTimers(N namespace) {
		return countTimersInNamespaceInternal(namespace, processingTimeTimersQueue);
	}

	//空间下注册的time数量
	@VisibleForTesting
	public int numEventTimeTimers(N namespace) {
		return countTimersInNamespaceInternal(namespace, eventTimeTimersQueue);
	}

	private int countTimersInNamespaceInternal(N namespace, InternalPriorityQueue<TimerHeapInternalTimer<K, N>> queue) {
		int count = 0;
		try (final CloseableIterator<TimerHeapInternalTimer<K, N>> iterator = queue.iterator()) {
			while (iterator.hasNext()) {
				final TimerHeapInternalTimer<K, N> timer = iterator.next();
				if (timer.getNamespace().equals(namespace)) {
					count++;
				}
			}
		} catch (Exception e) {
			throw new FlinkRuntimeException("Exception when closing iterator.", e);
		}
		return count;
	}

	//最小的分区序号
	@VisibleForTesting
	int getLocalKeyGroupRangeStartIdx() {
		return this.localKeyGroupRangeStartIdx;
	}

	//存储所有分区的数据到List中，每一个分区的数据是Set
	@VisibleForTesting
	List<Set<TimerHeapInternalTimer<K, N>>> getEventTimeTimersPerKeyGroup() {
		return partitionElementsByKeyGroup(eventTimeTimersQueue);
	}

	//存储所有分区的数据到List中，每一个分区的数据是Set
	@VisibleForTesting
	List<Set<TimerHeapInternalTimer<K, N>>> getProcessingTimeTimersPerKeyGroup() {
		return partitionElementsByKeyGroup(processingTimeTimersQueue);
	}

	//存储所有分区的数据到List中，每一个分区的数据是Set
	private <T> List<Set<T>> partitionElementsByKeyGroup(KeyGroupedInternalPriorityQueue<T> keyGroupedQueue) {
		List<Set<T>> result = new ArrayList<>(localKeyGroupRange.getNumberOfKeyGroups());//允许存放的key范围数量
		for (int keyGroup : localKeyGroupRange) {//循环每一个分区id
			result.add(Collections.unmodifiableSet(keyGroupedQueue.getSubsetForKeyGroup(keyGroup)));//存储每一个分区内的数据
		}
		return result;
	}

	private boolean areSnapshotSerializersIncompatible(InternalTimersSnapshot<?, ?> restoredSnapshot) {
		return (this.keyDeserializer != null && !this.keyDeserializer.equals(restoredSnapshot.getKeySerializer())) ||
			(this.namespaceDeserializer != null && !this.namespaceDeserializer.equals(restoredSnapshot.getNamespaceSerializer()));
	}
}
