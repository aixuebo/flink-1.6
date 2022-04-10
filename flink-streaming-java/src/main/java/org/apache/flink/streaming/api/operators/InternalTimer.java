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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.state.KeyExtractorFunction;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.runtime.state.PriorityComparable;
import org.apache.flink.runtime.state.PriorityComparator;

import javax.annotation.Nonnull;

/**
 * Internal interface for in-flight timers.
 *
 * @param <K> Type of the keys to which timers are scoped.可以获取key的值,K是key的类型
 * @param <N> Type of the namespace to which timers are scoped.
 *
 * 描述 key在空间N的到期时间,到期后会触发回调。
 * 因为可能会维护很多k在不同时间的回调,因此需要根据到期时间排序一下。
 *
 *
 * 1.可以按照触发的时间戳排序
 * 2.代表一个触发的任务,命名空间下,key在某个时间点要被触发。
 *
 *
 *  表示 在某个window窗口内的key,在某一个时间点被触发了。要做一些事儿，然后输出一些内容。
 */
@Internal
public interface InternalTimer<K, N> extends PriorityComparable<InternalTimer<?, ?>>, Keyed<K> {

	/** Function to extract the key from a {@link InternalTimer}. 提取key*/
	KeyExtractorFunction<InternalTimer<?, ?>> KEY_EXTRACTOR_FUNCTION = InternalTimer::getKey;

	/** Function to compare instances of {@link InternalTimer}. 比较时间戳大小*/
	PriorityComparator<InternalTimer<?, ?>> TIMER_COMPARATOR =
		(left, right) -> Long.compare(left.getTimestamp(), right.getTimestamp());
	/**
	 * Returns the timestamp of the timer. This value determines the point in time when the timer will fire.
	 * time被触发的时间戳
	 */
	long getTimestamp();

	/**
	 * Returns the key that is bound to this timer.
	 * 返回key对应的值
	 */
	@Nonnull
	@Override
	K getKey();

	/**
	 * Returns the namespace that is bound to this timer.
	 * 归属于哪个window窗口
	 */
	@Nonnull
	N getNamespace();
}
