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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.KeyExtractorFunction;
import org.apache.flink.runtime.state.KeyGroupPartitioner;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.RegisteredPriorityQueueStateBackendMetaInfo;
import org.apache.flink.runtime.state.StateSnapshot;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.lang.reflect.Array;

/**
 * This class represents the snapshot of an {@link HeapPriorityQueueSet}.
 *
 * @param <T> type of the state elements.
 *
 * 将元素存储在内存中，由于内存中存储的元素归属于多个partition,所以最终结果是按照partition的顺序,把元素一个个存储在内存数组中。
 * 因此 当需要输出某一个分区的数据时，只需要判断好该分区在内存数组的顺序区间，就可以获取该分区的数据了,输出到指定的输出流中
 */
public class HeapPriorityQueueStateSnapshot<T> implements StateSnapshot {

	/** Function that extracts keys from elements. */
	@Nonnull
	private final KeyExtractorFunction<T> keyExtractor;

	/** Copy of the heap array containing all the (immutable or deeply copied) elements. */
	@Nonnull
	private final T[] heapArrayCopy;

	/** The meta info of the state. */
	@Nonnull
	private final RegisteredPriorityQueueStateBackendMetaInfo<T> metaInfo;

	/** The key-group range covered by this snapshot. */
	@Nonnull
	private final KeyGroupRange keyGroupRange;

	/** The total number of key-groups in the job. */
	@Nonnegative
	private final int totalKeyGroups;

	/** Result of partitioning the snapshot by key-group. */
	@Nullable
	private StateKeyGroupWriter stateKeyGroupWriter;

	HeapPriorityQueueStateSnapshot(
		@Nonnull T[] heapArrayCopy,
		@Nonnull KeyExtractorFunction<T> keyExtractor,
		@Nonnull RegisteredPriorityQueueStateBackendMetaInfo<T> metaInfo,
		@Nonnull KeyGroupRange keyGroupRange,
		@Nonnegative int totalKeyGroups) {

		this.keyExtractor = keyExtractor;
		this.heapArrayCopy = heapArrayCopy;
		this.metaInfo = metaInfo;
		this.keyGroupRange = keyGroupRange;
		this.totalKeyGroups = totalKeyGroups;
	}

	@SuppressWarnings("unchecked")
	@Nonnull
	@Override
	public StateKeyGroupWriter getKeyGroupWriter() {

		if (stateKeyGroupWriter == null) {

			T[] partitioningOutput = (T[]) Array.newInstance(
				heapArrayCopy.getClass().getComponentType(),
				heapArrayCopy.length);

			final TypeSerializer<T> elementSerializer = metaInfo.getElementSerializer();//元素对象如何序列化成字节数组

			KeyGroupPartitioner<T> keyGroupPartitioner =
				new KeyGroupPartitioner<>(
					heapArrayCopy,//所有的数据元素
					heapArrayCopy.length,//元素数量
					partitioningOutput,//对heapArrayCopy进行排序,排序后的输出数组
					keyGroupRange,
					totalKeyGroups,////task节点上的partition数量
					keyExtractor,
					elementSerializer::serialize);////如何对数据处理,输出输出流中

			stateKeyGroupWriter = keyGroupPartitioner.partitionByKeyGroup();//返回partition输出对象
		}

		return stateKeyGroupWriter;
	}

	@Nonnull
	@Override
	public StateMetaInfoSnapshot getMetaInfoSnapshot() {
		return metaInfo.snapshot();
	}

	@Override
	public void release() {
	}
}
