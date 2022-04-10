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

import org.apache.flink.annotation.Internal;

/**
 * Interface for working with time and timers.
 *
 * <p>This is the internal version of {@link org.apache.flink.streaming.api.TimerService}
 * that allows to specify a key and a namespace to which timers should be scoped.
 *
 * @param <N> Type of the namespace to which timers are scoped.
 *
 * 时间服务,每一个N命名空间,独立存在,比如time点要产生回调,必须声明是N空间+time点产生回调
 *
 * 无论是windowOperator还是KeyedProcessOperator都持有InternalTimerService具体实现的对象，
 * 通过这个对象用户可以注册EventTime及ProcessTime的timer，当watermark 越过这些timer的时候，调用回调函数执行一定的操作。
 */
@Internal
public interface InternalTimerService<N> {

	/** Returns the current processing time.
	 * 获取当前时间戳
	 **/
	long currentProcessingTime();

	/** Returns the current event-time watermark.
	 * 获取水印
	 **/
	long currentWatermark();

	/**
	 * Registers a timer to be fired when processing time passes the given time. The namespace
	 * you pass here will be provided when the timer fires.
	 * 注册回调函数
	 */
	void registerProcessingTimeTimer(N namespace, long time);

	/**
	 * Deletes the timer for the given key and namespace.
	 */
	void deleteProcessingTimeTimer(N namespace, long time);

	/**
	 * Registers a timer to be fired when event time watermark passes the given time. The namespace
	 * you pass here will be provided when the timer fires.
	 */
	void registerEventTimeTimer(N namespace, long time);

	/**
	 * Deletes the timer for the given key and namespace.
	 */
	void deleteEventTimeTimer(N namespace, long time);
}
