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

import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

import java.nio.ByteBuffer;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Not thread safe class for filling in the content of the {@link MemorySegment}. To access written data please use
 * {@link BufferConsumer} which allows to build {@link Buffer} instances from the written data.
 *
 * 1.数据生产，将数据写入到MemorySegment。
 * 2.数据消费者，消费MemorySegment数据
 */
@NotThreadSafe
public class BufferBuilder {
	private final MemorySegment memorySegment;//内存--存储字节数组数据，支持将基础对象string、int等，转换成字节数组

	private final BufferRecycler recycler;

	private final SettablePositionMarker positionMarker = new SettablePositionMarker();

	private boolean bufferConsumerCreated = false;//true表示buffer的消费者已经创建完

	public BufferBuilder(MemorySegment memorySegment, BufferRecycler recycler) {
		this.memorySegment = checkNotNull(memorySegment);
		this.recycler = checkNotNull(recycler);
	}

	/**
	 * @return created matching instance of {@link BufferConsumer} to this {@link BufferBuilder}. There can exist only
	 * one {@link BufferConsumer} per each {@link BufferBuilder} and vice versa.
	 * 创建一个buffer的消费者
	 */
	public BufferConsumer createBufferConsumer() {
		checkState(!bufferConsumerCreated, "There can not exists two BufferConsumer for one BufferBuilder");
		bufferConsumerCreated = true;
		return new BufferConsumer(
			memorySegment,
			recycler,
			positionMarker);
	}

	//------数据向bufferConsumer生产数据
	/**
	 * Same as {@link #append(ByteBuffer)} but additionally {@link #commit()} the appending.
	 * 返回添加了多少个字节
	 */
	public int appendAndCommit(ByteBuffer source) {
		int writtenBytes = append(source);
		commit();
		return writtenBytes;
	}

	/**
	 * Append as many data as possible from {@code source}. Not everything might be copied if there is not enough
	 * space in the underlying {@link MemorySegment}
	 *
	 * @return number of copied bytes 返回写了多少个字节
	 * 因为容器最多填满memorySegment，所以source的数据量可能比容器容器memorySegment大
	 */
	public int append(ByteBuffer source) {
		checkState(!isFinished());

		int needed = source.remaining();//需要写入多少字节数组
		int available = getMaxCapacity() - positionMarker.getCached();//buffer剩余多少字节位置
		int toCopy = Math.min(needed, available);

		//将source输出到内存
		memorySegment.put(positionMarker.getCached(), source, toCopy);//从positionMarker.getCached()位置开始写入数据
		positionMarker.move(toCopy);//移动指针
		return toCopy;
	}

	/**
	 * Make the change visible to the readers. This is costly operation (volatile access) thus in case of bulk writes
	 * it's better to commit them all together instead one by one.
	 * 切换指针,设置position位置
	 */
	public void commit() {
		positionMarker.commit();
	}

	/**
	 * Mark this {@link BufferBuilder} and associated {@link BufferConsumer} as finished - no new data writes will be
	 * allowed.
	 *
	 * <p>This method should be idempotent to handle failures and task interruptions. Check FLINK-8948 for more details.
	 *
	 * @return number of written bytes.
	 */
	public int finish() {
		int writtenBytes = positionMarker.markFinished();
		commit();
		return writtenBytes;
	}

	public boolean isFinished() {
		return positionMarker.isFinished();
	}

	public boolean isFull() {
		checkState(positionMarker.getCached() <= getMaxCapacity());
		return positionMarker.getCached() == getMaxCapacity();
	}

	//返回内存容量
	public int getMaxCapacity() {
		return memorySegment.size();
	}

	/**
	 * Holds a reference to the current writer position. Negative values indicate that writer ({@link BufferBuilder}
	 * has finished. Value {@code Integer.MIN_VALUE} represents finished empty buffer.
	 */
	@ThreadSafe
	interface PositionMarker {
		int FINISHED_EMPTY = Integer.MIN_VALUE;//-2147483648

		int get();

		static boolean isFinished(int position) {
			return position < 0;
		}

		static int getAbsolute(int position) {
			if (position == FINISHED_EMPTY) {
				return 0;
			}
			return Math.abs(position);
		}
	}

	/**
	 * Cached writing implementation of {@link PositionMarker}.
	 *
	 * <p>Writer ({@link BufferBuilder}) and reader ({@link BufferConsumer}) caches must be implemented independently
	 * of one another - so that the cached values can not accidentally leak from one to another.
	 *
	 * <p>Remember to commit the {@link SettablePositionMarker} to make the changes visible.
	 *
	 * 调用流程 move + commit ，或者 move + markFinished + commit
	 *
	 1.在commit前,使用cachedPosition临时计数,此时position永远等于0;
	 2.写入数后,调用move(int offset),移动cachedPosition,此时cachedPosition永远>0.
	 3.当数据全部完成写入后,调用markFinished()方法,此时设置cachedPosition=-cachedPosition，即cachedPosition是负数。
	 4.因此调用boolean isFinished()方法，只有在调用markFinished()方法后，才会返回true，即已经写入完成。否则都是未完成。
	 5.commit() position = cachedPosition; 切换position。
	 *
	 */
	private static class SettablePositionMarker implements PositionMarker {
		private volatile int position = 0;

		/**
		 * Locally cached value of volatile {@code position} to avoid unnecessary volatile accesses.
		 */
		private int cachedPosition = 0;

		@Override
		public int get() {
			return position;
		}

		public boolean isFinished() {
			return PositionMarker.isFinished(cachedPosition);//负数说明已经完成
		}

		public int getCached() {
			return PositionMarker.getAbsolute(cachedPosition);
		}

		/**
		 * Marks this position as finished and returns the current position.
		 *
		 * @return current position as of {@link #getCached()}
		 */
		public int markFinished() {
			int currentPosition = getCached();
			int newValue = -currentPosition;
			if (newValue == 0) {
				newValue = FINISHED_EMPTY;
			}
			set(newValue);//设置成负数
			return currentPosition;
		}

		public void move(int offset) {
			set(cachedPosition + offset);
		}

		public void set(int value) {
			cachedPosition = value;
		}

		//切换
		public void commit() {
			position = cachedPosition;
		}
	}
}
