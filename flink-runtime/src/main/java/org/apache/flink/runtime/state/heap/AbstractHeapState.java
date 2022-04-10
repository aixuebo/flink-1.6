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

package org.apache.flink.runtime.state.heap;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.util.Preconditions;

/**
 * Base class for partitioned {@link State} implementations that are backed by a regular
 * heap hash map. The concrete implementations define how the state is checkpointed.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <SV> The type of the values in the state.
 *  满足基础 -- 如何序列化/反序列化key、命名空间、vakue值。  如果通过key+命名空间,获取value值对象
 *
 *
 * 1.StateTable:、key由K+命名空间对象组成,value由value的对象组成。
 * 2.为了支持字节数组与对象的转换,因此需要有key、value、命名空间的序列化/反序列化对象。
 * 3.持有一个key-value容器,用于存储节点内所有的key
 *
 */
public abstract class AbstractHeapState<K, N, SV> implements InternalKvState<K, N, SV> {

	/** Map containing the actual key/value pairs. 存储key+value的容器 */
	protected final StateTable<K, N, SV> stateTable;

	/** The current namespace, which the access methods will refer to. 当前的命名空间,即归属于哪个window*/
	protected N currentNamespace;

	//key和value如何序列化/反序列化
	protected final TypeSerializer<K> keySerializer;

	protected final TypeSerializer<SV> valueSerializer;

	protected final TypeSerializer<N> namespaceSerializer;

	private final SV defaultValue;//默认值

	/**
	 * Creates a new key/value state for the given hash map of key/value pairs.
	 *
	 * @param stateTable The state table for which this state is associated to.存储key+value的容器
	 * @param keySerializer The serializer for the keys.key和value如何序列化/反序列化
	 * @param valueSerializer The serializer for the state.
	 * @param namespaceSerializer The serializer for the namespace.
	 * @param defaultValue The default value for the state.状态的默认值
	 */
	AbstractHeapState(
			StateTable<K, N, SV> stateTable,
			TypeSerializer<K> keySerializer,
			TypeSerializer<SV> valueSerializer,
			TypeSerializer<N> namespaceSerializer,
			SV defaultValue) {

		this.stateTable = Preconditions.checkNotNull(stateTable, "State table must not be null.");
		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;
		this.namespaceSerializer = namespaceSerializer;
		this.defaultValue = defaultValue;
		this.currentNamespace = null;
	}

	// ------------------------------------------------------------------------

	//删除该key对应的值
	@Override
	public final void clear() {
		stateTable.remove(currentNamespace);
	}

	@Override
	public final void setCurrentNamespace(N namespace) {
		this.currentNamespace = Preconditions.checkNotNull(namespace, "Namespace must not be null.");
	}

	//从容器中获取key对应的value字节数组,然后反序列化成value对象
	//相当于get方法
	@Override
	public byte[] getSerializedValue(
			final byte[] serializedKeyAndNamespace,//key+命名空间组成的字节数组
			final TypeSerializer<K> safeKeySerializer,
			final TypeSerializer<N> safeNamespaceSerializer,
			final TypeSerializer<SV> safeValueSerializer) throws Exception {

		Preconditions.checkNotNull(serializedKeyAndNamespace);
		Preconditions.checkNotNull(safeKeySerializer);
		Preconditions.checkNotNull(safeNamespaceSerializer);
		Preconditions.checkNotNull(safeValueSerializer);

		Tuple2<K, N> keyAndNamespace = KvStateSerializer.deserializeKeyAndNamespace(
				serializedKeyAndNamespace, safeKeySerializer, safeNamespaceSerializer);//反序列化得到key+命名空间对象

		SV result = stateTable.get(keyAndNamespace.f0, keyAndNamespace.f1);//获取命名空间映射的value

		if (result == null) {
			return null;
		}
		return KvStateSerializer.serializeValue(result, safeValueSerializer);//反序列化value
	}

	/**
	 * This should only be used for testing.
	 * 返回容器
	 */
	@VisibleForTesting
	public StateTable<K, N, SV> getStateTable() {
		return stateTable;
	}

	//获取value的默认值
	protected SV getDefaultValue() {
		if (defaultValue != null) {
			return valueSerializer.copy(defaultValue);//重复利用value对象
		} else {
			return null;
		}
	}
}
