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

package org.apache.flink.runtime.io.network.buffer;

import org.apache.flink.core.memory.MemorySegment;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufAllocator;

import java.nio.ByteBuffer;

/**
 * Wrapper for pooled {@link MemorySegment} instances with reference counting.
 *
 * <p>This is similar to Netty's <tt>ByteBuf</tt> with some extensions and restricted to the methods
 * our use cases outside Netty handling use. In particular, we use two different indexes for read
 * and write operations, i.e. the <tt>reader</tt> and <tt>writer</tt> index (size of written data),
 * which specify three regions inside the memory segment:
 * <pre>
 *     +-------------------+----------------+----------------+
 *     | discardable bytes | readable bytes | writable bytes |
 *     +-------------------+----------------+----------------+
 *     |                   |                |                |
 *     0      <=      readerIndex  <=  writerIndex   <=  max capacity
 * </pre>
 *
 * <p>Our non-Netty usages of this <tt>Buffer</tt> class either rely on the underlying {@link
 * #getMemorySegment()} directly, or on {@link ByteBuffer} wrappers of this buffer which do not
 * modify either index, so the indices need to be updated manually via {@link #setReaderIndex(int)}
 * and {@link #setSize(int)}.
 *
 * 提供2个能力
 * 1.重复使用该buffer，每一次持有者可以增加一个buffer的引用。
 * 2.有2个index,readerIndex和writerIndex，readerIndex之前的可以被丢弃了，writerIndex-readerIndex是未来可以读取的。
 */
public interface Buffer {

	/**
	 * Returns whether this buffer represents a buffer or an event.
	 *
	 * @return <tt>true</tt> if this is a real buffer, <tt>false</tt> if this is an event
	 * true代表是buffer,false代表不是buffer,而是一个event,事件表示特殊含义
	 */
	boolean isBuffer();

	/**
	 * Tags this buffer to represent an event.
	 * 表示event,即设置isBuffer = false
	 */
	void tagAsEvent();

	/**
	 * Returns the underlying memory segment. This method is dangerous since it ignores read only protections and omits
	 * slices. Use it only along the {@link #getMemorySegmentOffset()}.
	 *
	 * <p>This method will be removed in the future. For writing use {@link BufferBuilder}.
	 *
	 * @return the memory segment backing this buffer
	 * 返回buffer对应的内存对象,从该对象获取字节、int等信息 --- 从该对象中读取数据
	 */
	@Deprecated
	MemorySegment getMemorySegment();

	/**
	 * This method will be removed in the future. For writing use {@link BufferBuilder}.
	 * 该方法未来会被删除
	 * @return the offset where this (potential slice) {@link Buffer}'s data start in the underlying memory segment.
	 * 表示MemorySegment的开始位置
	 */
	@Deprecated
	int getMemorySegmentOffset();

	/**
	 * Gets the buffer's recycler.
	 *
	 * @return buffer recycler
	 * 回收该内存对象的对象
	 */
	BufferRecycler getRecycler();

	/**
	 * Releases this buffer once, i.e. reduces the reference count and recycles the buffer if the
	 * reference count reaches <tt>0</tt>.
	 *
	 * @see #retainBuffer()
	 * 减少一个引用，当引用对象是0的时候，就可以回收了
	 */
	void recycleBuffer();

	/**
	 * Returns whether this buffer has been recycled or not.
	 *
	 * @return <tt>true</tt> if already recycled, <tt>false</tt> otherwise
	 * 是否可回收 -- 即引用对象是否是0
	 */
	boolean isRecycled();

	/**
	 * Retains this buffer for further use, increasing the reference counter by <tt>1</tt>.
	 *
	 * @return <tt>this</tt> instance (for chained calls)
	 *
	 * @see #recycleBuffer()
	 * 保留缓冲器，未来使用，同时引用增加1
	 */
	Buffer retainBuffer();

	/**
	 * Returns a read-only slice of this buffer's readable bytes, i.e. between
	 * {@link #getReaderIndex()} and {@link #getSize()}.
	 *
	 * <p>Reader and writer indices as well as markers are not shared. Reference counters are
	 * shared but the slice is not {@link #retainBuffer() retained} automatically.
	 *
	 * @return a read-only sliced buffer
	 * 返回一个只读的buffer，读取index开始 - 所有可读的字节信息
	 */
	Buffer readOnlySlice();

	/**
	 * Returns a read-only slice of this buffer.
	 *
	 * <p>Reader and writer indices as well as markers are not shared. Reference counters are
	 * shared but the slice is not {@link #retainBuffer() retained} automatically.
	 *
	 * @param index the index to start from
	 * @param length the length of the slice
	 *
	 * @return a read-only sliced buffer
	 * 返回一个只读的buffer，读取index开始，length长度字节
	 */
	Buffer readOnlySlice(int index, int length);

	/**
	 * Returns the maximum size of the buffer, i.e. the capacity of the underlying {@link MemorySegment}.
	 *
	 * @return size of the buffer
	 * buffer最大的容量,该容量不指代已经存储的数据量,只表示容量
	 */
	int getMaxCapacity();

	/**
	 * Returns the <tt>reader index</tt> of this buffer.
	 *
	 * <p>This is where readable (unconsumed) bytes start in the backing memory segment.
	 *
	 * @return reader index (from 0 (inclusive) to the size of the backing {@link MemorySegment}
	 * (inclusive))
	 * 获取可读的index位置
	 */
	int getReaderIndex();

	/**
	 * Sets the <tt>reader index</tt> of this buffer.
	 *
	 * @throws IndexOutOfBoundsException
	 * 		if the index is less than <tt>0</tt> or greater than {@link #getSize()}
	 * 设置reader的index
	 */
	void setReaderIndex(int readerIndex) throws IndexOutOfBoundsException;

	/**
	 * Returns the size of the written data, i.e. the <tt>writer index</tt>, of this buffer in an
	 * non-synchronized fashion.
	 *
	 * <p>This is where writable bytes start in the backing memory segment.
	 *
	 * @return writer index (from 0 (inclusive) to the size of the backing {@link MemorySegment}
	 * (inclusive))
	 * 非安全的方式获取writerIndex
	 */
	int getSizeUnsafe();

	/**
	 * Returns the size of the written data, i.e. the <tt>writer index</tt>, of this buffer.
	 *
	 * <p>This is where writable bytes start in the backing memory segment.
	 *
	 * @return writer index (from 0 (inclusive) to the size of the backing {@link MemorySegment}
	 * (inclusive))
	 * 获取writerIndex位置
	 */
	int getSize();

	/**
	 * Sets the size of the written data, i.e. the <tt>writer index</tt>, of this buffer.
	 *
	 * @throws IndexOutOfBoundsException
	 * 		if the index is less than {@link #getReaderIndex()} or greater than {@link #getMaxCapacity()}
	 * 设置writerIndex
	 */
	void setSize(int writerIndex);

	/**
	 * Returns the number of readable bytes (same as <tt>{@link #getSize()} -
	 * {@link #getReaderIndex()}</tt>).
	 * this.writerIndex - this.readerIndex; 还可以读取多少个字节
	 */
	int readableBytes();

	/**
	 * Gets a new {@link ByteBuffer} instance wrapping this buffer's readable bytes, i.e. between
	 * {@link #getReaderIndex()} and {@link #getSize()}.
	 *
	 * <p>Please note that neither index is updated by the returned buffer.
	 *
	 * @return byte buffer sharing the contents of the underlying memory segment
	 */
	ByteBuffer getNioBufferReadable();

	/**
	 * Gets a new {@link ByteBuffer} instance wrapping this buffer's bytes.
	 *
	 * <p>Please note that neither <tt>read</tt> nor <tt>write</tt> index are updated by the
	 * returned buffer.
	 *
	 * @return byte buffer sharing the contents of the underlying memory segment
	 *
	 * @throws IndexOutOfBoundsException
	 * 		if the indexes are not without the buffer's bounds
	 * @see #getNioBufferReadable()
	 * 裁剪成一个新的buffer对象,只有length个字节
	 */
	ByteBuffer getNioBuffer(int index, int length) throws IndexOutOfBoundsException;

	/**
	 * Sets the buffer allocator for use in netty.
	 *
	 * @param allocator netty buffer allocator
	 * buffer分配器
	 */
	void setAllocator(ByteBufAllocator allocator);

	/**
	 * @return self as ByteBuf implementation.
	 */
	ByteBuf asByteBuf();
}
