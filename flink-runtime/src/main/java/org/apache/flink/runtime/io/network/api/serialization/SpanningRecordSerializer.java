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
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Record serializer which serializes the complete record to an intermediate
 * data serialization buffer and copies this buffer to target buffers
 * one-by-one using {@link #continueWritingWithNextBufferBuilder(BufferBuilder)}.
 *
 * @param <T> The type of the records that are serialized.
 *
 * 数据拆分成两部分，一个数据大小的buffer、一个data buffer，输出到memorySegment中
 * 返回结果是 该条数据是否输出完成，容器是否满了
 *
 * 持续输出记录
 */
public class SpanningRecordSerializer<T extends IOReadableWritable> implements RecordSerializer<T> {

	/** Flag to enable/disable checks, if buffer not set/full or pending serialization. */
	private static final boolean CHECKED = false;//是否启动检查功能

	/** Intermediate data serialization. */
	private final DataOutputSerializer serializationBuffer;//输出流--输出的是字节数组，但可以直接传入int、string等基础信息,自动会转换成字节数组

	/** Intermediate buffer for data serialization (wrapped from {@link #serializationBuffer}). */
	private ByteBuffer dataBuffer;//用于存储数据的中间结果buffer

	/** Intermediate buffer for length serialization. */
	private final ByteBuffer lengthBuffer;//用于存储中间结果的长度，4个字节

	/** Current target {@link Buffer} of the serializer. */
	@Nullable
	private BufferBuilder targetBuffer;//背后是MemorySegment,一整块大内存

	public SpanningRecordSerializer() {
		serializationBuffer = new DataOutputSerializer(128);//创建一个buffer输出流,初始化128个字节

		lengthBuffer = ByteBuffer.allocate(4);
		lengthBuffer.order(ByteOrder.BIG_ENDIAN);

		// ensure initial state with hasRemaining false (for correct continueWritingWithNextBufferBuilder logic)
		dataBuffer = serializationBuffer.wrapAsByteBuffer();//输出流转换成ByteBuffer 用于存储数据
		lengthBuffer.position(4);
	}

	/**
	 * Serializes the complete record to an intermediate data serialization
	 * buffer and starts copying it to the target buffer (if available).
	 *
	 * @param record the record to serialize
	 * @return how much information was written to the target buffer and
	 *         whether this buffer is full
	 */
	@Override
	public SerializationResult addRecord(T record) throws IOException {
		if (CHECKED) {
			if (dataBuffer.hasRemaining()) {
				throw new IllegalStateException("Pending serialization of previous record.");
			}
		}

		serializationBuffer.clear();
		lengthBuffer.clear();

		// write data and length
		record.write(serializationBuffer);//向内存byte[]输出数据

		int len = serializationBuffer.length();
		lengthBuffer.putInt(0, len);

		dataBuffer = serializationBuffer.wrapAsByteBuffer();//返回写入的byte数组内容

		// Copy from intermediate buffers to current target memory segment
		if (targetBuffer != null) {//初始化的时候没有该buffer容器，所以返回结果是PARTIAL_RECORD_MEMORY_SEGMENT_FULL
			targetBuffer.append(lengthBuffer);
			targetBuffer.append(dataBuffer);
			targetBuffer.commit();
		}

		return getSerializationResult();
	}

	//更换下一个buffer容器 --- 将数据写入到BufferBuilder中
	@Override
	public SerializationResult continueWritingWithNextBufferBuilder(BufferBuilder buffer) throws IOException {
		targetBuffer = buffer;

		boolean mustCommit = false;
		if (lengthBuffer.hasRemaining()) {
			targetBuffer.append(lengthBuffer);
			mustCommit = true;
		}

		if (dataBuffer.hasRemaining()) {
			targetBuffer.append(dataBuffer);
			mustCommit = true;
		}

		if (mustCommit) {
			targetBuffer.commit();
		}

		SerializationResult result = getSerializationResult();

		// make sure we don't hold onto the large buffers for too long
		if (result.isFullRecord()) {//true说明一条完整的数据都写入了,缓存的内容已经无意义了
			serializationBuffer.clear();
			serializationBuffer.pruneBuffer();//如果缓存太大了,则缩小一下缓存空间
			dataBuffer = serializationBuffer.wrapAsByteBuffer();
		}

		return result;
	}

	private SerializationResult getSerializationResult() {
		if (dataBuffer.hasRemaining() || lengthBuffer.hasRemaining()) {
			return SerializationResult.PARTIAL_RECORD_MEMORY_SEGMENT_FULL;
		}
		return !targetBuffer.isFull()
				? SerializationResult.FULL_RECORD
				: SerializationResult.FULL_RECORD_MEMORY_SEGMENT_FULL;
	}

	@Override
	public void clear() {
		targetBuffer = null;
	}

	//true 表示已经写入序列化数据到字节数组中
	@Override
	public boolean hasSerializedData() {
		return lengthBuffer.hasRemaining() || dataBuffer.hasRemaining();
	}
}
