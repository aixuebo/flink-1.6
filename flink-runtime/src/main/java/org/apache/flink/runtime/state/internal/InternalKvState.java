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

package org.apache.flink.runtime.state.internal;

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.typeutils.TypeSerializer;

/**
 * The {@code InternalKvState} is the root of the internal state type hierarchy, similar to the
 * {@link State} being the root of the public API state hierarchy.
 * 
 * <p>The internal state classes give access to the namespace getters and setters and access to
 * additional functionality, like raw value access or state merging.
 * 
 * <p>The public API state hierarchy is intended to be programmed against by Flink applications.
 * The internal state hierarchy holds all the auxiliary methods that are used by the runtime and not
 * intended to be used by user applications. These internal methods are considered of limited use to users and
 * only confusing, and are usually not regarded as stable across releases.
 * 
 * <p>Each specific type in the internal state hierarchy extends the type from the public
 * state hierarchy:
 * 
 * <pre>
 *             State
 *               |
 *               +-------------------InternalKvState
 *               |                         |
 *          MergingState                   |
 *               |                         |
 *               +-----------------InternalMergingState
 *               |                         |
 *      +--------+------+                  |
 *      |               |                  |
 * ReducingState    ListState        +-----+-----------------+
 *      |               |            |                       |
 *      +-----------+   +-----------   -----------------InternalListState
 *                  |                |
 *                  +---------InternalReducingState
 * </pre>
 *
 * @param <K> The type of key the state is associated to
 * @param <N> The type of the namespace
 * @param <V> The type of values kept internally in state
 *
 * 内存中存储的永远都是 <(K,N),V) 组成的对象,但传输中需要传输的是字节数组,因此需要序列化与反序列化操作
 */
public interface InternalKvState<K, N, V> extends State {

	/**
	 * Returns the {@link TypeSerializer} for the type of key this state is associated to.
	 */
	TypeSerializer<K> getKeySerializer();

	/**
	 * Returns the {@link TypeSerializer} for the type of namespace this state is associated to.
	 */
	TypeSerializer<N> getNamespaceSerializer();

	/**
	 * Returns the {@link TypeSerializer} for the type of value this state holds.
	 */
	TypeSerializer<V> getValueSerializer();

	/**
	 * Sets the current namespace, which will be used when using the state access methods.
	 *
	 * @param namespace The namespace.
	 */
	void setCurrentNamespace(N namespace);

	/**
	 * Returns the serialized value for the given key and namespace.
	 *
	 * <p>If no value is associated with key and namespace, <code>null</code>
	 * is returned.
	 *
	 * <p><b>TO IMPLEMENTERS:</b> This method is called by multiple threads. Anything
	 * stateful (e.g. serializers) should be either duplicated or protected from undesired
	 * consequences of concurrent invocations.
	 *
	 * @param serializedKeyAndNamespace Serialized key and namespace
	 * @param safeKeySerializer A key serializer which is safe to be used even in multi-threaded context
	 * @param safeNamespaceSerializer A namespace serializer which is safe to be used even in multi-threaded context
	 * @param safeValueSerializer A value serializer which is safe to be used even in multi-threaded context
	 * @return Serialized value or <code>null</code> if no value is associated with the key and namespace.
	 * 
	 * @throws Exception Exceptions during serialization are forwarded
	 * 对value进行序列化 --- 通过该key+命名空间,找到value值,然后再序列化value
	 *
	 * 相当于Map的get方法,传入key,获取value
	 *
	 * 首先将key反序列化成key+命名空间对象。然后通过key+命名空间对象,获取value对象,然后将value对象反序列化
	 *
	 * 相当于get方法,只是返回的是传输的二进制,即value经过序列化后的值
	 */
	byte[] getSerializedValue(
			final byte[] serializedKeyAndNamespace,//key+命名空间的序列化数据
			final TypeSerializer<K> safeKeySerializer,
			final TypeSerializer<N> safeNamespaceSerializer,
			final TypeSerializer<V> safeValueSerializer) //获取value对象,然后将其转换成字节数组,用于传输
		throws Exception;
}
