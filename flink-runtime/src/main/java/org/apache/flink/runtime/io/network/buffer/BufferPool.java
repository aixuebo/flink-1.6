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

import java.io.IOException;

/**
 * A dynamically sized buffer pool.
 * 代表一个动态的buffer pool
 */
public interface BufferPool extends BufferProvider, BufferRecycler {

	/**
	 * The owner of this buffer pool to be called when memory needs to be released to avoid back
	 * pressure.
	 * 该buffer pool的拥有者,只有该拥有者才能释放内存
	 */
	void setBufferPoolOwner(BufferPoolOwner owner);

	/**
	 * Destroys this buffer pool.
	 *
	 * <p> If not all buffers are available, they are recycled lazily as soon as they are recycled.
	 */
	void lazyDestroy();

	/**
	 * Checks whether this buffer pool has been destroyed.
	 * 是否已经销毁,销毁了是不能再拿到内存buffer对象的
	 */
	@Override
	boolean isDestroyed();

	/**
	 * Returns the number of guaranteed (minimum number of) memory segments of this buffer pool.
	 * 可以从该池子里最少拿到多少个内存对象
	 */
	int getNumberOfRequiredMemorySegments();

	/**
	 * Returns the maximum number of memory segments this buffer pool should use
	 *
	 * @return maximum number of memory segments to use or <tt>-1</tt> if unlimited
	 * 可以从该池子里最多拿到多少个内存对象
	 */
	int getMaxNumberOfMemorySegments();

	/**
	 * Returns the current size of this buffer pool.
	 *
	 * <p> The size of the buffer pool can change dynamically at runtime.
	 * 缓冲池大小，即允许缓冲区可以有多少个内存对象，不代表都被使用了
	 */
	int getNumBuffers();

	/**
	 * Sets the current size of this buffer pool.
	 *
	 * <p> The size needs to be greater or equal to the guaranteed number of memory segments.
	 */
	void setNumBuffers(int numBuffers) throws IOException;

	/**
	 * Returns the number memory segments, which are currently held by this buffer pool.
	 * 当前可用的数量
	 */
	int getNumberOfAvailableMemorySegments();

	/**
	 * Returns the number of used buffers of this buffer pool.
	 * 返回多少个被使用的缓冲区
	 */
	int bestEffortGetNumOfUsedBuffers();
}
