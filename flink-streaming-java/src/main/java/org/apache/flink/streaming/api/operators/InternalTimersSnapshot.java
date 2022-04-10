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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Set;

/**
 * A snapshot of internal timers, containing event and processing timers and
 * the serializers to use to write / read them.
 *
 * InternalTimers的快照,包含所有的timers序列化的内容，如何序列化/反序列化这些数据
 *
 * 快照包含的内容--说明已经反序列化完成的对象
 */
public class InternalTimersSnapshot<K, N> {

	//key和命名空间N如何序列化,用于具体序列化TimerSerializer的key和命名空间
	private TypeSerializer<K> keySerializer;
	private TypeSerializerConfigSnapshot keySerializerConfigSnapshot;
	private TypeSerializer<N> namespaceSerializer;
	private TypeSerializerConfigSnapshot namespaceSerializerConfigSnapshot;

	//TimerSerializer 对象用于 序列化K和N 以及 时间戳,描述 key在空间N的到期时间,到期后会触发回调。
	private Set<TimerHeapInternalTimer<K, N>> eventTimeTimers;
	private Set<TimerHeapInternalTimer<K, N>> processingTimeTimers;

	/** Empty constructor used when restoring the timers. */
	public InternalTimersSnapshot() {}

	/** Constructor to use when snapshotting the timers. */
	public InternalTimersSnapshot(
			TypeSerializer<K> keySerializer,
			TypeSerializerConfigSnapshot keySerializerConfigSnapshot,
			TypeSerializer<N> namespaceSerializer,
			TypeSerializerConfigSnapshot namespaceSerializerConfigSnapshot,
			@Nullable Set<TimerHeapInternalTimer<K, N>> eventTimeTimers,//允许是null
			@Nullable Set<TimerHeapInternalTimer<K, N>> processingTimeTimers) {

		this.keySerializer = Preconditions.checkNotNull(keySerializer);
		this.keySerializerConfigSnapshot = Preconditions.checkNotNull(keySerializerConfigSnapshot);
		this.namespaceSerializer = Preconditions.checkNotNull(namespaceSerializer);
		this.namespaceSerializerConfigSnapshot = Preconditions.checkNotNull(namespaceSerializerConfigSnapshot);
		this.eventTimeTimers = eventTimeTimers;
		this.processingTimeTimers = processingTimeTimers;
	}

	public TypeSerializer<K> getKeySerializer() {
		return keySerializer;
	}

	public void setKeySerializer(TypeSerializer<K> keySerializer) {
		this.keySerializer = keySerializer;
	}

	public TypeSerializerConfigSnapshot getKeySerializerConfigSnapshot() {
		return keySerializerConfigSnapshot;
	}

	public void setKeySerializerConfigSnapshot(TypeSerializerConfigSnapshot keySerializerConfigSnapshot) {
		this.keySerializerConfigSnapshot = keySerializerConfigSnapshot;
	}

	public TypeSerializer<N> getNamespaceSerializer() {
		return namespaceSerializer;
	}

	public void setNamespaceSerializer(TypeSerializer<N> namespaceSerializer) {
		this.namespaceSerializer = namespaceSerializer;
	}

	public TypeSerializerConfigSnapshot getNamespaceSerializerConfigSnapshot() {
		return namespaceSerializerConfigSnapshot;
	}

	public void setNamespaceSerializerConfigSnapshot(TypeSerializerConfigSnapshot namespaceSerializerConfigSnapshot) {
		this.namespaceSerializerConfigSnapshot = namespaceSerializerConfigSnapshot;
	}

	//TimerSerializer 对象用于 序列化K和N 以及 时间戳,描述 key在空间N的到期时间,到期后会触发回调。
	public Set<TimerHeapInternalTimer<K, N>> getEventTimeTimers() {
		return eventTimeTimers;
	}

	public void setEventTimeTimers(Set<TimerHeapInternalTimer<K, N>> eventTimeTimers) {
		this.eventTimeTimers = eventTimeTimers;
	}

	//TimerSerializer 对象用于 序列化K和N 以及 时间戳,描述 key在空间N的到期时间,到期后会触发回调。
	public Set<TimerHeapInternalTimer<K, N>> getProcessingTimeTimers() {
		return processingTimeTimers;
	}

	public void setProcessingTimeTimers(Set<TimerHeapInternalTimer<K, N>> processingTimeTimers) {
		this.processingTimeTimers = processingTimeTimers;
	}

	@Override
	public boolean equals(Object obj) {
		return super.equals(obj);
	}

	@Override
	public int hashCode() {
		return super.hashCode();
	}
}
