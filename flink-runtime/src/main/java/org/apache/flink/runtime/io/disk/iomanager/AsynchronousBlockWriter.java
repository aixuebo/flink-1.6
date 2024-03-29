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

package org.apache.flink.runtime.io.disk.iomanager;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.flink.core.memory.MemorySegment;

//内部实现了一个回调函数,收集已经写入完成的数据块,对外提供获取每一个完成的数据块,并且销毁等操作
public class AsynchronousBlockWriter extends AsynchronousBlockWriterWithCallback implements BlockChannelWriter<MemorySegment> {
	
	private final LinkedBlockingQueue<MemorySegment> returnSegments;//缓存已经写入成功的segment
	
	/**
	 * Creates a new block channel writer for the given channel.
	 *  
	 * @param channelID The ID of the channel to write to.输出的文件
	 * @param requestQueue The request queue of the asynchronous writer thread, to which the I/O requests
	 *                     are added.请求队列
	 * @param returnSegments The return queue, to which the processed Memory Segments are added.缓存已经写入成功的segment
	 * @throws IOException Thrown, if the underlying file channel could not be opened exclusively.
	 */
	protected AsynchronousBlockWriter(FileIOChannel.ID channelID, RequestQueue<WriteRequest> requestQueue,
			LinkedBlockingQueue<MemorySegment> returnSegments)
	throws IOException
	{
		super(channelID, requestQueue, new QueuingCallback<MemorySegment>(returnSegments));//回调函数啥也不做,只是把处理完成的数据块MemorySegment存储到队列returnSegments里
		this.returnSegments = returnSegments;
	}
	
	/**
	 * Gets the next memory segment that has been written and is available again.
	 * This method blocks until such a segment is available, or until an error occurs in the writer, or the
	 * writer is closed.
	 * <p>
	 * NOTE: If this method is invoked without any segment ever returning (for example, because the
	 * {@link #writeBlock(MemorySegment)} method has not been invoked accordingly), the method may block
	 * forever.
	 * 
	 * @return The next memory segment from the writers's return queue.
	 * @throws IOException Thrown, if an I/O error occurs in the writer while waiting for the request to return.
	 * 返回一个写入完成的数据块
	 */
	@Override
	public MemorySegment getNextReturnedBlock() throws IOException {
		try {
			while (true) {
				final MemorySegment next = returnSegments.poll(1000, TimeUnit.MILLISECONDS);
				if (next != null) {
					return next;
				} else {
					if (this.closed) {
						throw new IOException("The writer has been closed.");
					}
					checkErroneous();
				}
			}
		} catch (InterruptedException e) {
			throw new IOException("Writer was interrupted while waiting for the next returning segment.");
		}
	}
	
	/**
	 * Gets the queue in which the memory segments are queued after the asynchronous write is completed.
	 * 
	 * @return The queue with the written memory segments.
	 */
	@Override
	public LinkedBlockingQueue<MemorySegment> getReturnQueue() {
		return this.returnSegments;
	}
}
