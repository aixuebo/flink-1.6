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

package org.apache.flink.runtime.state;

import org.apache.flink.runtime.state.StateSnapshotTransformer.CollectionStateSnapshotTransformer.TransformStrategy;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.apache.flink.runtime.state.StateSnapshotTransformer.CollectionStateSnapshotTransformer.TransformStrategy.STOP_ON_FIRST_INCLUDED;

/**
 * Transformer of state values which are included or skipped in the snapshot.
 *
 * <p>This transformer can be applied to state values
 * to decide which entries should be included into the snapshot.
 * The included entries can be optionally modified before.
 *
 * <p>Unless specified differently, the transformer should be applied per entry
 * for collection types of state, like list or map.
 *
 * @param <T> type of state
 *
 * 对分区内的数据做一下过滤，不是所有数据都要做快照
 */
@FunctionalInterface
public interface StateSnapshotTransformer<T> {
	/**
	 * Transform or filter out state values which are included or skipped in the snapshot.
	 *
	 * @param value non-serialized form of value
	 * @return value to snapshot or null which means the entry is not included
	 *
	 * 对state原始内容做处理。
	 * 1.输入value是state原始内容
	 * 2.将原始value做格式转换,然后再存储。
	 * 3.或者对原始value做判断，如果不想存储,则设置为null,就会将其过滤掉,不会存储在快照中
	 */
	@Nullable
	T filterOrTransform(@Nullable T value);

	/** Collection state specific transformer which says how to transform entries of the collection. */
	interface CollectionStateSnapshotTransformer<T> extends StateSnapshotTransformer<T> {
		enum TransformStrategy {
			/** Transform all entries. 全部元素都参与转换*/
			TRANSFORM_ALL,

			/**
			 * Skip first null entries.第一个元素被转换了,则后面元素都不需要再转换了，原封不动的添加进去就可以
			 *
			 * <p>While traversing collection entries, as optimisation, stops transforming
			 * if encounters first non-null included entry and returns it plus the rest untouched.
			 */
			STOP_ON_FIRST_INCLUDED
		}

		default TransformStrategy getFilterStrategy() {
			return TransformStrategy.TRANSFORM_ALL;
		}
	}

	/**
	 * General implementation of list state transformer.
	 *
	 * <p>This transformer wraps a transformer per-entry
	 * and transforms the whole list state.
	 * If the wrapped per entry transformer is {@link CollectionStateSnapshotTransformer},
	 * it respects its {@link TransformStrategy}.
	 *
	 * 对list类型的value进行转换处理,因为list中的value可能在转换过程中会被丢弃
	 */
	class ListStateSnapshotTransformer<T> implements StateSnapshotTransformer<List<T>> {
		private final StateSnapshotTransformer<T> entryValueTransformer;//list中的每一个元素如何处理转换操作
		private final TransformStrategy transformStrategy;

		public ListStateSnapshotTransformer(StateSnapshotTransformer<T> entryValueTransformer) {
			this.entryValueTransformer = entryValueTransformer;
			this.transformStrategy = entryValueTransformer instanceof CollectionStateSnapshotTransformer ?
				((CollectionStateSnapshotTransformer) entryValueTransformer).getFilterStrategy() :
				TransformStrategy.TRANSFORM_ALL;
		}

		@Override
		@Nullable
		public List<T> filterOrTransform(@Nullable List<T> list) {
			if (list == null) {
				return null;
			}
			List<T> transformedList = new ArrayList<>();
			boolean anyChange = false;
			for (int i = 0; i < list.size(); i++) {
				T entry = list.get(i);
				T transformedEntry = entryValueTransformer.filterOrTransform(entry);//对list中的元素进行转换处理
				if (transformedEntry != null) {
					if (transformStrategy == STOP_ON_FIRST_INCLUDED) {//第一个元素被转换了,则后面元素都不需要再转换了，原封不动的添加进去就可以
						transformedList = list.subList(i, list.size());
						anyChange = i > 0;
						break;
					} else {
						transformedList.add(transformedEntry);
					}
				}
				anyChange |= transformedEntry == null || !Objects.equals(entry, transformedEntry);//true 说明转换起了作用
			}
			transformedList = anyChange ? transformedList : list;//存储换换后的数据 还是 没有转换的数据
			return transformedList.isEmpty() ? null : transformedList;
		}
	}

	/**
	 * General implementation of map state transformer.
	 *
	 * <p>This transformer wraps a transformer per-entry
	 * and transforms the whole map state.
	 *
	 * 对map类型的value进行转换处理,因为map中的value可能在转换过程中会被丢弃
	 */
	class MapStateSnapshotTransformer<K, V> implements StateSnapshotTransformer<Map<K, V>> {
		private final StateSnapshotTransformer<V> entryValueTransformer;

		public MapStateSnapshotTransformer(StateSnapshotTransformer<V> entryValueTransformer) {
			this.entryValueTransformer = entryValueTransformer;
		}

		@Nullable
		@Override
		public Map<K, V> filterOrTransform(@Nullable Map<K, V> map) {
			if (map == null) {
				return null;
			}
			Map<K, V> transformedMap = new HashMap<>();
			boolean anyChange = false;
			for (Map.Entry<K, V> entry : map.entrySet()) {
				V transformedValue = entryValueTransformer.filterOrTransform(entry.getValue());
				if (transformedValue != null) {
					transformedMap.put(entry.getKey(), transformedValue);
				}
				anyChange |= transformedValue == null || !Objects.equals(entry.getValue(), transformedValue); //true表示经过转换,返回值是null或者已经跟原始值不一样了
			}
			return anyChange ? (transformedMap.isEmpty() ? null : transformedMap) : map;//有更改,则存储更改后的数据,没有更改,则存储map原本内容
		}
	}

	/**
	 * This factory creates state transformers depending on the form of values to transform.
	 *
	 * <p>If there is no transforming needed, the factory methods return {@code Optional.empty()}.
	 */
	interface StateSnapshotTransformFactory<T> {
		StateSnapshotTransformFactory<?> NO_TRANSFORM = createNoTransform();

		@SuppressWarnings("unchecked")
		static <T> StateSnapshotTransformFactory<T> noTransform() {
			return (StateSnapshotTransformFactory<T>) NO_TRANSFORM;
		}

		static <T> StateSnapshotTransformFactory<T> createNoTransform() {
			return new StateSnapshotTransformFactory<T>() {
				@Override
				public Optional<StateSnapshotTransformer<T>> createForDeserializedState() {
					return Optional.empty();
				}

				@Override
				public Optional<StateSnapshotTransformer<byte[]>> createForSerializedState() {
					return Optional.empty();
				}
			};
		}

		Optional<StateSnapshotTransformer<T>> createForDeserializedState();

		Optional<StateSnapshotTransformer<byte[]>> createForSerializedState();
	}
}
