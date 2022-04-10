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

package org.apache.flink.streaming.api;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Interface for working with time and timers.
 * 时间服务器,可以获取当前处理时间戳、水印时间戳。同时可以注册延后处理服务
 */
@PublicEvolving
public interface TimerService {

	/** Error string for {@link UnsupportedOperationException} on registering timers. */
	String UNSUPPORTED_REGISTER_TIMER_MSG = "Setting timers is only supported on a keyed streams.";

	/** Error string for {@link UnsupportedOperationException} on deleting timers. */
	String UNSUPPORTED_DELETE_TIMER_MSG = "Deleting timers is only supported on a keyed streams.";

	/** Returns the current processing time.
	 * 返回当前时间戳,表示处理数据的当前时间 System.currentTimeMillis(),即表示处理元素的时间戳
	 **/
	long currentProcessingTime();

	/** Returns the current event-time watermark.
	 * 返回当前事件的watermark值
	 **/
	long currentWatermark();

	/**
	 * Registers a timer to be fired when processing time passes the given time.
	 *
	 * <p>Timers can internally be scoped to keys and/or windows. When you set a timer
	 * in a keyed context, such as in an operation on
	 * {@link org.apache.flink.streaming.api.datastream.KeyedStream} then that context
	 * will also be active when you receive the timer notification.
	 * 注册回调函数
	 */
	void registerProcessingTimeTimer(long time);

	/**
	 * Registers a timer to be fired when the event time watermark passes the given time.
	 *
	 * <p>Timers can internally be scoped to keys and/or windows. When you set a timer
	 * in a keyed context, such as in an operation on
	 * {@link org.apache.flink.streaming.api.datastream.KeyedStream} then that context
	 * will also be active when you receive the timer notification.
	 */
	void registerEventTimeTimer(long time);

	/**
	 * Deletes the processing-time timer with the given trigger time. This method has only an effect if such a timer
	 * was previously registered and did not already expire.
	 *
	 * <p>Timers can internally be scoped to keys and/or windows. When you delete a timer,
	 * it is removed from the current keyed context.
	 */
	void deleteProcessingTimeTimer(long time);

	/**
	 * Deletes the event-time timer with the given trigger time. This method has only an effect if such a timer
	 * was previously registered and did not already expire.
	 *
	 * <p>Timers can internally be scoped to keys and/or windows. When you delete a timer,
	 * it is removed from the current keyed context.
	 */
	void deleteEventTimeTimer(long time);
}
