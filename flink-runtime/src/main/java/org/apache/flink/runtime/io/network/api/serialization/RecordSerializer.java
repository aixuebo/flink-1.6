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

package org.apache.flink.runtime.io.network.api.serialization;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;

import java.io.IOException;

/**
 * Interface for turning records into sequences of memory segments.
 * 如何将一条元素,转换成二进制,输出到memory segments中
 *
 *
 * 数据先写入byte字节数组中,然后写入到MemorySegment大内存块中
 */
public interface RecordSerializer<T extends IOReadableWritable> {

	/**
	 * Status of the serialization result.序列化的结果
	 */
	enum SerializationResult {
		//数据一定没有全部输出,因为内存满了,不足以输出数据内容
		PARTIAL_RECORD_MEMORY_SEGMENT_FULL(false, true),//数据写了一部分，因为buffer满了

		//数据全部输出, 由于内存是否写满不一定,因此有两个分类
		FULL_RECORD_MEMORY_SEGMENT_FULL(true, true),//一条数据输出完成，同时buffer满了
		FULL_RECORD(true, false);//一条记录输出完成

		private final boolean isFullRecord;//true说明一条完整的数据都写入了

		private final boolean isFullBuffer;//true 说明 容器被写满了

		SerializationResult(boolean isFullRecord, boolean isFullBuffer) {
			this.isFullRecord = isFullRecord;
			this.isFullBuffer = isFullBuffer;
		}

		/**
		 * Whether the full record was serialized and completely written to
		 * a target buffer.
		 * true表示一条记录都输出完成
		 * @return <tt>true</tt> if the complete record was written
		 */
		public boolean isFullRecord() {
			return this.isFullRecord;
		}

		/**
		 * Whether the target buffer is full after the serialization process.
		 *
		 * @return <tt>true</tt> if the target buffer is full
		 * true表示目标缓冲池已经满了
		 */
		public boolean isFullBuffer() {
			return this.isFullBuffer;
		}
	}

	/**
	 * Starts serializing and copying the given record to the target buffer
	 * (if available).
	 *
	 * @param record the record to serialize
	 * @return how much information was written to the target buffer and
	 *         whether this buffer is full
	 * 执行写入数据,返回写入状态
	 */
	SerializationResult addRecord(T record) throws IOException;

	/**
	 * Sets a (next) target buffer to use and continues writing remaining data
	 * to it until it is full.
	 *
	 * @param bufferBuilder the new target buffer to use
	 * @return how much information was written to the target buffer and
	 *         whether this buffer is full
	 */
	SerializationResult continueWritingWithNextBufferBuilder(BufferBuilder bufferBuilder) throws IOException;

	/**
	 * Clear and release internal state.
	 */
	void clear();

	/**
	 * @return <tt>true</tt> if has some serialized data pending copying to the result {@link BufferBuilder}.
	 * true 表示已经写入序列化数据到字节数组中,还尚未写入到BufferBuilder
	 */
	boolean hasSerializedData();
}
