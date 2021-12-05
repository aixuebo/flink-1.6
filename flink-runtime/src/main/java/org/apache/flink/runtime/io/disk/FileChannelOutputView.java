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

package org.apache.flink.runtime.io.disk;

import java.io.IOException;
import java.util.List;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.iomanager.BlockChannelWriter;
import org.apache.flink.runtime.memory.AbstractPagedOutputView;
import org.apache.flink.runtime.memory.MemoryManager;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link org.apache.flink.core.memory.DataOutputView} that is backed by a {@link BlockChannelWriter}, making it effectively a data output
 * stream. The view writes it data in blocks to the underlying channel.
 *
 * 正常的向segment中写入数据，写满后输出到file中。
 * 外界正常使用write方法即可。内部把逻辑封装起来
 *
 * 步骤:
 * 1.初始化一组List<MemorySegment>,用于临时存储字节数组。
 * 2.正常调用write方法写入数据到MemorySegment。
 * 3.当MemorySegment写满后,异步的调用writeSegment(current, posInSegment)写入到文件中。
 * 4.返回下一个可用的MemorySegment,再次存储数据，重复2-4步骤。
 * 5.最终close方法，将所有segment数据写入到文件中，并且回收MemorySegment的内存
 */
public class FileChannelOutputView extends AbstractPagedOutputView {
	
	private final BlockChannelWriter<MemorySegment> writer;		// the writer to the channel  如何将segment写入到文件中
	
	private final MemoryManager memManager;//生产释放MemorySegment内存
	
	private final List<MemorySegment> memory;
	
	private int numBlocksWritten;//一共写了多少个segment
	
	private int bytesInLatestSegment;//上一个segment最后写的偏移量位置,因为每一个segment的size是固定值,因此只有最后一个segment结尾位置是不知道的
	
	// --------------------------------------------------------------------------------------------

	/**
	 *
	 * @param writer 代表一个文件,如何向该文件写入数据
	 * @param memManager 用于释放MemorySegment
	 * @param memory 用于存储数据的总集合，存储的数据不会超过这个范围
	 * @param segmentSize  每一个segment的固定大小
	 * @throws IOException
	 */
	public FileChannelOutputView(BlockChannelWriter<MemorySegment> writer, MemoryManager memManager, List<MemorySegment> memory, int segmentSize) throws IOException {
		super(segmentSize, 0);
		
		checkNotNull(writer);
		checkNotNull(memManager);
		checkNotNull(memory);
		checkArgument(!writer.isClosed());
		
		this.writer = writer;
		this.memManager = memManager;
		this.memory = memory;
		
		//添加N个内存空间,后期使用
		for (MemorySegment next : memory) {
			writer.getReturnQueue().add(next);
		}
		
		// move to the first page
		advance();
	}
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Closes this output, writing pending data and releasing the memory.
	 * 
	 * @throws IOException Thrown, if the pending data could not be written.
	 */
	public void close() throws IOException {
		close(false);
	}
	
	/**
	 * Closes this output, writing pending data and releasing the memory.
	 * 
	 * @throws IOException Thrown, if the pending data could not be written.
	 */
	public void closeAndDelete() throws IOException {
		close(true);
	}
	
	private void close(boolean delete) throws IOException {
		try {
			// send off set last segment, if we have not been closed before
			MemorySegment current = getCurrentSegment();//最后一个segment数据写入到文件
			if (current != null) {
				writeSegment(current, getCurrentPositionInSegment());
			}

			clear();
			if (delete) {
				writer.closeAndDelete();
			} else {
				writer.close();
			}
		}
		finally {
			memManager.release(memory);//释放内存
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Gets the number of blocks written by this output view.
	 * 
	 * @return The number of blocks written by this output view.
	 * 一共写了多少个segment
	 */
	public int getBlockCount() {
		return numBlocksWritten;
	}
	
	/**
	 * Gets the number of bytes written in the latest memory segment.
	 * 上一个segment最后写的偏移量位置
	 * @return The number of bytes written in the latest memory segment.
	 */
	public int getBytesInLatestSegment() {
		return bytesInLatestSegment;
	}

	//返回一共写了多少个字节
	public long getWriteOffset() {
		return ((long) numBlocksWritten) * segmentSize + getCurrentPositionInSegment();
	}

	//将参数current与当前current写入的位置posInSegment,进行保存,然后返回一个新的segment,下游可以继续写入数据
	@Override
	protected MemorySegment nextSegment(MemorySegment current, int posInSegment) throws IOException {
		if (current != null) {
			writeSegment(current, posInSegment);
		}
		return writer.getNextReturnedBlock();
	}
	
	private void writeSegment(MemorySegment segment, int writePosition) throws IOException {
		writer.writeBlock(segment);//做一个异步请求,将segment数据块内容写入到文件
		numBlocksWritten++;
		bytesInLatestSegment = writePosition;
	}
}
