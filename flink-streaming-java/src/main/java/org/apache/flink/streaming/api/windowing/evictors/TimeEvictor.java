/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.windowing.evictors;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;

import java.util.Iterator;

/**
 * An {@link Evictor} that keeps elements for a certain amount of time. Elements older
 * than {@code current_time - keep_time} are evicted. The current_time is time associated
 * with {@link TimestampedValue}
 *
 * @param <W> The type of {@link Window Windows} on which this {@code Evictor} can operate.
 * 以时间为判断标准，决定元素是否会被移除
 */
@PublicEvolving
public class TimeEvictor<W extends Window> implements Evictor<Object, W> {
	private static final long serialVersionUID = 1L;

	private final long windowSize;//时间窗口，保留windowSize时间范围内的数据。
	private final boolean doEvictAfter;//默认是false,即evictBefore进行删除数据

	//trigger 触发后,保留windowSize时间范围内的数据。
	public TimeEvictor(long windowSize) {
		this.windowSize = windowSize;
		this.doEvictAfter = false;
	}

	public TimeEvictor(long windowSize, boolean doEvictAfter) {
		this.windowSize = windowSize;
		this.doEvictAfter = doEvictAfter;
	}

	@Override
	public void evictBefore(Iterable<TimestampedValue<Object>> elements, int size, W window, EvictorContext ctx) {
		if (!doEvictAfter) {
			evict(elements, size, ctx);
		}
	}

	@Override
	public void evictAfter(Iterable<TimestampedValue<Object>> elements, int size, W window, EvictorContext ctx) {
		if (doEvictAfter) {
			evict(elements, size, ctx);
		}
	}

	//删除掉windowSize之前的数据
	private void evict(Iterable<TimestampedValue<Object>> elements, int size, EvictorContext ctx) {
		if (!hasTimestamp(elements)) {//没有时间戳,则直接返回
			return;
		}

		long currentTime = getMaxTimestamp(elements);//返回集合中最大的时间戳 --- 一次循环
		long evictCutoff = currentTime - windowSize; //比如windowSize为1小时

		for (Iterator<TimestampedValue<Object>> iterator = elements.iterator(); iterator.hasNext(); ) {//又一次循环
			TimestampedValue<Object> record = iterator.next();
			if (record.getTimestamp() <= evictCutoff) { //删除掉1小时前的数据
				iterator.remove();
			}
		}
	}

	/**
	 * Returns true if the first element in the Iterable of {@link TimestampedValue} has a timestamp.
	 * true表示元素有时间戳
     */
	private boolean hasTimestamp(Iterable<TimestampedValue<Object>> elements) {
		Iterator<TimestampedValue<Object>> it = elements.iterator();
		if (it.hasNext()) {
			return it.next().hasTimestamp();
		}
		return false;
	}

	/**
	 * @param elements The elements currently in the pane.
	 * @return The maximum value of timestamp among the elements.
	 * 返回集合中最大的时间戳
     */
	private long getMaxTimestamp(Iterable<TimestampedValue<Object>> elements) {
		long currentTime = Long.MIN_VALUE;
		for (Iterator<TimestampedValue<Object>> iterator = elements.iterator(); iterator.hasNext();){
			TimestampedValue<Object> record = iterator.next();
			currentTime = Math.max(currentTime, record.getTimestamp());
		}
		return currentTime;
	}

	@Override
	public String toString() {
		return "TimeEvictor(" + windowSize + ")";
	}

	@VisibleForTesting
	public long getWindowSize() {
		return windowSize;
	}

	/**
	 * Creates a {@code TimeEvictor} that keeps the given number of elements.
	 * Eviction is done before the window function.
	 *
	 * @param windowSize The amount of time for which to keep elements.
	 */
	public static <W extends Window> TimeEvictor<W> of(Time windowSize) {
		return new TimeEvictor<>(windowSize.toMilliseconds());
	}

	/**
	 * Creates a {@code TimeEvictor} that keeps the given number of elements.
	 * Eviction is done before/after the window function based on the value of doEvictAfter.
	 *
	 * @param windowSize The amount of time for which to keep elements.
	 * @param doEvictAfter Whether eviction is done after window function.
     */
	public static <W extends Window> TimeEvictor<W> of(Time windowSize, boolean doEvictAfter) {
		return new TimeEvictor<>(windowSize.toMilliseconds(), doEvictAfter);
	}
}
