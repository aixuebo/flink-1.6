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
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.runtime.state.KeyGroupPartitioner;
import org.apache.flink.runtime.state.StateSnapshotKeyGroupReader;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import java.io.IOException;

/**
 * This class provides a static factory method to create different implementations of {@link StateSnapshotKeyGroupReader}
 * depending on the provided serialization format version.
 * <p>
 * The implementations are also located here as inner classes.
 */
class StateTableByKeyGroupReaders {

	/**
	 * Creates a new StateTableByKeyGroupReader that inserts de-serialized mappings into the given table, using the
	 * de-serialization algorithm that matches the given version.
	 *
	 * @param stateTable the {@link StateTable} into which de-serialized mappings are inserted.
	 * @param version version for the de-serialization algorithm.
	 * @param <K> type of key.
	 * @param <N> type of namespace.
	 * @param <S> type of state.
	 * @return the appropriate reader.
	 * 如何读取StateTable
	 */
	static <K, N, S> StateSnapshotKeyGroupReader readerForVersion(StateTable<K, N, S> stateTable, int version) {
		switch (version) {
			case 1:
				return new StateTableByKeyGroupReaderV1<>(stateTable);
			case 2:
			case 3:
			case 4:
			case 5:
				return createV2PlusReader(stateTable);
			default:
				throw new IllegalArgumentException("Unknown version: " + version);
		}
	}

	//组成方式 byte[命名空间] + byte[key] + byte[value]
	private static <K, N, S> StateSnapshotKeyGroupReader createV2PlusReader(StateTable<K, N, S> stateTable) {
		final TypeSerializer<K> keySerializer = stateTable.keyContext.getKeySerializer();
		final TypeSerializer<N> namespaceSerializer = stateTable.getNamespaceSerializer();
		final TypeSerializer<S> stateSerializer = stateTable.getStateSerializer();
		final Tuple3<N, K, S> buffer = new Tuple3<>();
		return KeyGroupPartitioner.createKeyGroupPartitionReader((in) -> {//反序列化三元组
			buffer.f0 = namespaceSerializer.deserialize(in);
			buffer.f1 = keySerializer.deserialize(in);
			buffer.f2 = stateSerializer.deserialize(in);
			return buffer;
		}, (element, keyGroupId1) -> stateTable.put(element.f1, keyGroupId1, element.f0, element.f2));//key、keyGroupId, namespace, value
	}

	//填充StateTable
	static final class StateTableByKeyGroupReaderV1<K, N, S> implements StateSnapshotKeyGroupReader {

		protected final StateTable<K, N, S> stateTable;

		StateTableByKeyGroupReaderV1(StateTable<K, N, S> stateTable) {
			this.stateTable = stateTable;
		}

		@Override
		public void readMappingsInKeyGroup(@Nonnull DataInputView inView, @Nonnegative int keyGroupId) throws IOException {

			if (inView.readByte() == 0) {
				return;
			}

			final TypeSerializer<K> keySerializer = stateTable.keyContext.getKeySerializer();
			final TypeSerializer<N> namespaceSerializer = stateTable.getNamespaceSerializer();
			final TypeSerializer<S> stateSerializer = stateTable.getStateSerializer();

			// V1 uses kind of namespace compressing format
			int numNamespaces = inView.readInt();//多少个不同的命名空间
			for (int k = 0; k < numNamespaces; k++) {
				N namespace = namespaceSerializer.deserialize(inView);//反序列化 命名空间
				int numEntries = inView.readInt();//有多少个key=value映射
				for (int l = 0; l < numEntries; l++) {
					K key = keySerializer.deserialize(inView);//反序列化key对象
					S state = stateSerializer.deserialize(inView);//反序列化value
					stateTable.put(key, keyGroupId, namespace, state);
				}
			}
		}
	}
}
