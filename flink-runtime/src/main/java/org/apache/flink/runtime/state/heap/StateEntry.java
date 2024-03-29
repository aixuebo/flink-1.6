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

package org.apache.flink.runtime.state.heap;

/**
 * Interface of entries in a state table. Entries are triple of key, namespace, and state.
 *
 * @param <K> type of key.
 * @param <N> type of namespace.
 * @param <S> type of state.
 * 三元组:因为存储快照,有时候是需要group分组后,针对每一个key有一个对象存储数据的。
 * 因此key+State(存储key的对象,比如list、map等)是必须的，同时key还有命名空间,所以是三元组
 */
public interface StateEntry<K, N, S> {

	/**
	 * Returns the key of this entry.
	 */
	K getKey();

	/**
	 * Returns the namespace of this entry.
	 */
	N getNamespace();

	/**
	 * Returns the state of this entry.
	 */
	S getState();
}
