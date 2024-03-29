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

import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.MathUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The NetworkBufferPool is a fixed size pool of {@link MemorySegment} instances
 * for the network stack.
 *
 * <p>The NetworkBufferPool creates {@link LocalBufferPool}s from which the individual tasks draw
 * the buffers for the network data transfer. When new local buffer pools are created, the
 * NetworkBufferPool dynamically redistributes the buffers between the pools.
 *
 * 1.构造函数，分配totalNumberOfMemorySegments/numberOfSegmentsToAllocate个MemorySegment对象,存储在availableMemorySegments里。每一个是segmentSize大小
 * 2.MemorySegment requestMemorySegment() 从队列中返回一个MemorySegment对象
 * 3.recycle(MemorySegment segment) 向队列中添加一个MemorySegment对象,内存不释放。
 * recycleMemorySegments(List<MemorySegment> segments)
 * 4.destroy() 释放内存
 *
 */
public class NetworkBufferPool implements BufferPoolFactory {

	private static final Logger LOG = LoggerFactory.getLogger(NetworkBufferPool.class);

	private final int totalNumberOfMemorySegments;//需要多少个MemorySegment内存对象--应该分配的最多数

	private final int memorySegmentSize;//每一个MemorySegment占用内存大小

	private final ArrayBlockingQueue<MemorySegment> availableMemorySegments;//分配的内存池,等待被使用

	private volatile boolean isDestroyed;//true表示销毁,不再支持buffer内存提供 --- 释放availableMemorySegments所占用的内存

	// ---- Managed buffer pools ----------------------------------------------

	private final Object factoryLock = new Object();//锁对象

	private final Set<LocalBufferPool> allBufferPools = new HashSet<>();//分配的每一个小的缓冲池

	private int numTotalRequiredBuffers;//已经分配了多少个MemorySegment

	/**
	 * 构造函数，分配totalNumberOfMemorySegments/numberOfSegmentsToAllocate个MemorySegment对象,存储在availableMemorySegments里。每一个是segmentSize大小
	 * Allocates all {@link MemorySegment} instances managed by this pool.
	 * 分配所有的MemorySegments内存对象,初始化的时候创建好所有的MemorySegments内存对象，等待分配
	 * @numberOfSegmentsToAllocate 分配多少个MemorySegments内存对象
	 * @segmentSize 每一个MemorySegments需要多少内存
	 */
	public NetworkBufferPool(int numberOfSegmentsToAllocate, int segmentSize) {

		this.totalNumberOfMemorySegments = numberOfSegmentsToAllocate;
		this.memorySegmentSize = segmentSize;

		final long sizeInLong = (long) segmentSize;

		try {
			this.availableMemorySegments = new ArrayBlockingQueue<>(numberOfSegmentsToAllocate);
		}
		catch (OutOfMemoryError err) {
			throw new OutOfMemoryError("Could not allocate buffer queue of length "
					+ numberOfSegmentsToAllocate + " - " + err.getMessage());
		}

		//预分配MemorySegments集合
		try {
			for (int i = 0; i < numberOfSegmentsToAllocate; i++) {
				ByteBuffer memory = ByteBuffer.allocateDirect(segmentSize);
				availableMemorySegments.add(MemorySegmentFactory.wrapPooledOffHeapMemory(memory, null));
			}
		}
		catch (OutOfMemoryError err) {//内存溢出
			int allocated = availableMemorySegments.size();//已经分配了多少个MemorySegment对象

			// free some memory
			availableMemorySegments.clear();

			long requiredMb = (sizeInLong * numberOfSegmentsToAllocate) >> 20;//要求请求多少内存 单位M
			long allocatedMb = (sizeInLong * allocated) >> 20;//已经确定分配了多少内存 单位M
			long missingMb = requiredMb - allocatedMb;//确实多少M内存

			throw new OutOfMemoryError("Could not allocate enough memory segments for NetworkBufferPool " +
					"(required (Mb): " + requiredMb +
					", allocated (Mb): " + allocatedMb +
					", missing (Mb): " + missingMb + "). Cause: " + err.getMessage());
		}

		long allocatedMb = (sizeInLong * availableMemorySegments.size()) >> 20;//打印日志,已经分配了内存多少M

		LOG.info("Allocated {} MB for network buffer pool (number of memory segments: {}, bytes per segment: {}).",
				allocatedMb, availableMemorySegments.size(), segmentSize);
	}

	//返回一个可用的MemorySegment内存对象
	@Nullable
	public MemorySegment requestMemorySegment() {
		return availableMemorySegments.poll();
	}

	//回收一个MemorySegment
	public void recycle(MemorySegment segment) {
		// Adds the segment back to the queue, which does not immediately free the memory
		// however, since this happens when references to the global pool are also released,
		// making the availableMemorySegments queue and its contained object reclaimable
		availableMemorySegments.add(checkNotNull(segment));
	}

	//返回参数个MemorySegment对象
	public List<MemorySegment> requestMemorySegments(int numRequiredBuffers) throws IOException {
		checkArgument(numRequiredBuffers > 0, "The number of required buffers should be larger than 0.");

		synchronized (factoryLock) {
			if (isDestroyed) {
				throw new IllegalStateException("Network buffer pool has already been destroyed.");
			}

			if (numTotalRequiredBuffers + numRequiredBuffers > totalNumberOfMemorySegments) {//空间不足分配
				throw new IOException(String.format("Insufficient number of network buffers: " +
								"required %d, but only %d available. The total number of network " +
								"buffers is currently set to %d of %d bytes each. You can increase this " +
								"number by setting the configuration keys '%s', '%s', and '%s'.",
						numRequiredBuffers,
						totalNumberOfMemorySegments - numTotalRequiredBuffers,
						totalNumberOfMemorySegments,
						memorySegmentSize,
						TaskManagerOptions.NETWORK_BUFFERS_MEMORY_FRACTION.key(),
						TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MIN.key(),
						TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MAX.key()));
			}

			this.numTotalRequiredBuffers += numRequiredBuffers;

			try {
				redistributeBuffers();
			} catch (Throwable t) {
				this.numTotalRequiredBuffers -= numRequiredBuffers;

				try {
					redistributeBuffers();
				} catch (IOException inner) {
					t.addSuppressed(inner);
				}
				ExceptionUtils.rethrowIOException(t);
			}
		}

		//返回分配的MemorySegment
		final List<MemorySegment> segments = new ArrayList<>(numRequiredBuffers);
		try {
			while (segments.size() < numRequiredBuffers) {
				if (isDestroyed) {
					throw new IllegalStateException("Buffer pool is destroyed.");
				}

				final MemorySegment segment = availableMemorySegments.poll(2, TimeUnit.SECONDS);
				if (segment != null) {
					segments.add(segment);
				}
			}
		} catch (Throwable e) {
			try {
				recycleMemorySegments(segments, numRequiredBuffers);
			} catch (IOException inner) {
				e.addSuppressed(inner);
			}
			ExceptionUtils.rethrowIOException(e);
		}

		return segments;
	}

	//回收MemorySegment资源
	public void recycleMemorySegments(List<MemorySegment> segments) throws IOException {
		recycleMemorySegments(segments, segments.size());
	}

	//回收MemorySegment资源
	private void recycleMemorySegments(List<MemorySegment> segments, int size) throws IOException {
		synchronized (factoryLock) {
			numTotalRequiredBuffers -= size;

			availableMemorySegments.addAll(segments);

			// note: if this fails, we're fine for the buffer pool since we already recycled the segments
			redistributeBuffers();
		}
	}

	//销毁
	public void destroy() {
		synchronized (factoryLock) {
			isDestroyed = true;

			MemorySegment segment;
			while ((segment = availableMemorySegments.poll()) != null) {
				segment.free();
			}
		}
	}

	public boolean isDestroyed() {
		return isDestroyed;
	}

	public int getMemorySegmentSize() {
		return memorySegmentSize;
	}

	public int getTotalNumberOfMemorySegments() {
		return totalNumberOfMemorySegments;
	}

	//有效的可以被利用的MemorySegment内存对象数量
	public int getNumberOfAvailableMemorySegments() {
		return availableMemorySegments.size();
	}

	public int getNumberOfRegisteredBufferPools() {
		synchronized (factoryLock) {
			return allBufferPools.size();
		}
	}

	public int countBuffers() {
		int buffers = 0;

		synchronized (factoryLock) {
			for (BufferPool bp : allBufferPools) {
				buffers += bp.getNumBuffers();
			}
		}

		return buffers;
	}

	// ------------------------------------------------------------------------
	// BufferPoolFactory
	// ------------------------------------------------------------------------
    //创建一个子buffer缓冲池LocalBufferPool
	@Override
	public BufferPool createBufferPool(int numRequiredBuffers, int maxUsedBuffers) throws IOException {
		// It is necessary to use a separate lock from the one used for buffer
		// requests to ensure deadlock freedom for failure cases.
		synchronized (factoryLock) {
			if (isDestroyed) {
				throw new IllegalStateException("Network buffer pool has already been destroyed.");
			}

			// Ensure that the number of required buffers can be satisfied.
			// With dynamic memory management this should become obsolete.
			if (numTotalRequiredBuffers + numRequiredBuffers > totalNumberOfMemorySegments) {
				throw new IOException(String.format("Insufficient number of network buffers: " +
								"required %d, but only %d available. The total number of network " +
								"buffers is currently set to %d of %d bytes each. You can increase this " +
								"number by setting the configuration keys '%s', '%s', and '%s'.",
						numRequiredBuffers,
						totalNumberOfMemorySegments - numTotalRequiredBuffers,
						totalNumberOfMemorySegments,
						memorySegmentSize,
						TaskManagerOptions.NETWORK_BUFFERS_MEMORY_FRACTION.key(),
						TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MIN.key(),
						TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MAX.key()));
			}

			this.numTotalRequiredBuffers += numRequiredBuffers;

			// We are good to go, create a new buffer pool and redistribute
			// non-fixed size buffers.
			LocalBufferPool localBufferPool =
				new LocalBufferPool(this, numRequiredBuffers, maxUsedBuffers);

			allBufferPools.add(localBufferPool);

			try {
				redistributeBuffers();
			} catch (IOException e) {
				try {
					destroyBufferPool(localBufferPool);
				} catch (IOException inner) {
					e.addSuppressed(inner);
				}
				ExceptionUtils.rethrowIOException(e);
			}

			return localBufferPool;
		}
	}

	//销毁某一个子缓冲区LocalBufferPool
	@Override
	public void destroyBufferPool(BufferPool bufferPool) throws IOException {
		if (!(bufferPool instanceof LocalBufferPool)) {
			throw new IllegalArgumentException("bufferPool is no LocalBufferPool");
		}

		synchronized (factoryLock) {
			if (allBufferPools.remove(bufferPool)) {
				numTotalRequiredBuffers -= bufferPool.getNumberOfRequiredMemorySegments();

				redistributeBuffers();
			}
		}
	}

	/**
	 * Destroys all buffer pools that allocate their buffers from this
	 * buffer pool (created via {@link #createBufferPool(int, int)}).
	 */
	public void destroyAllBufferPools() {
		synchronized (factoryLock) {
			// create a copy to avoid concurrent modification exceptions
			LocalBufferPool[] poolsCopy = allBufferPools.toArray(new LocalBufferPool[allBufferPools.size()]);

			for (LocalBufferPool pool : poolsCopy) {
				pool.lazyDestroy();
			}

			// some sanity checks
			if (allBufferPools.size() > 0 || numTotalRequiredBuffers > 0) {
				throw new IllegalStateException("NetworkBufferPool is not empty after destroying all LocalBufferPools");
			}
		}
	}

	// Must be called from synchronized block
	//重新分配内存数
	//重新设置每一个LocalBufferPool应该可以超出使用多少个segment
	private void redistributeBuffers() throws IOException {
		assert Thread.holdsLock(factoryLock);

		// All buffers, which are not among the required ones
		final int numAvailableMemorySegment = totalNumberOfMemorySegments - numTotalRequiredBuffers;//剩余多少个MemorySegment

		if (numAvailableMemorySegment == 0) {//没有剩余公用的内存可以被子缓冲池复用,因此每一个缓冲池只能有自己应该有的数量
			// in this case, we need to redistribute buffers so that every pool gets its minimum
			for (LocalBufferPool bufferPool : allBufferPools) {
				bufferPool.setNumBuffers(bufferPool.getNumberOfRequiredMemorySegments());
			}
			return;
		}

		/*
		 * With buffer pools being potentially limited, let's distribute the available memory
		 * segments based on the capacity of each buffer pool, i.e. the maximum number of segments
		 * an unlimited buffer pool can take is numAvailableMemorySegment, for limited buffer pools
		 * it may be less. Based on this and the sum of all these values (totalCapacity), we build
		 * a ratio that we use to distribute the buffers.
		 */

		long totalCapacity = 0; // long to avoid int overflow 还需要多少空间

		for (LocalBufferPool bufferPool : allBufferPools) {
			int excessMax = bufferPool.getMaxNumberOfMemorySegments() -
				bufferPool.getNumberOfRequiredMemorySegments();//允许超出的最大数
			totalCapacity += Math.min(numAvailableMemorySegment, excessMax);
		}

		// no capacity to receive additional buffers?
		if (totalCapacity == 0) {
			return; // necessary to avoid div by zero when nothing to re-distribute
		}

		// since one of the arguments of 'min(a,b)' is a positive int, this is actually
		// guaranteed to be within the 'int' domain
		// (we use a checked downCast to handle possible bugs more gracefully).
		final int memorySegmentsToDistribute = MathUtils.checkedDownCast(
				Math.min(numAvailableMemorySegment, totalCapacity));

		long totalPartsUsed = 0; // of totalCapacity
		int numDistributedMemorySegment = 0;
		for (LocalBufferPool bufferPool : allBufferPools) {
			int excessMax = bufferPool.getMaxNumberOfMemorySegments() -
				bufferPool.getNumberOfRequiredMemorySegments();//允许超出的最大数

			// shortcut
			if (excessMax == 0) {
				continue;
			}

			totalPartsUsed += Math.min(numAvailableMemorySegment, excessMax);

			// avoid remaining buffers by looking at the total capacity that should have been
			// re-distributed up until here
			// the downcast will always succeed, because both arguments of the subtraction are in the 'int' domain
			final int mySize = MathUtils.checkedDownCast(
					memorySegmentsToDistribute * totalPartsUsed / totalCapacity - numDistributedMemorySegment);

			numDistributedMemorySegment += mySize;
			bufferPool.setNumBuffers(bufferPool.getNumberOfRequiredMemorySegments() + mySize);
		}

		assert (totalPartsUsed == totalCapacity);
		assert (numDistributedMemorySegment == memorySegmentsToDistribute);
	}
}
