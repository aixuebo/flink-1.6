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

import java.io.EOFException;
import java.io.IOException;
import java.util.List;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.iomanager.BlockChannelReader;
import org.apache.flink.runtime.memory.AbstractPagedInputView;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.util.MathUtils;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A {@link org.apache.flink.core.memory.DataInputView} that is backed by a {@link BlockChannelReader},
 * making it effectively a data input stream. The view reads it data in blocks from the underlying channel.
 * The view can read data that has been written by a {@link FileChannelOutputView}, or that was written in blocks
 * in another fashion.
 *
 * 读取一个文件,一次性读取List<MemorySegment> memory个数据回来,慢慢消化。调用getString等方式可以正常读取数据
 *
 *
 * 1.初始化一组List<MemorySegment>,用于临时存储读取的字节数组。
 * 2.正常调用reader.readBlock(seg)方法,异步读取数据到MemorySegment。
 * 3.不断调用read方法读取字节内容。当segment没有内容时,调用下一个方法
 * 4.MemorySegment nextSegment(MemorySegment current) 获取下一个读满的segment
 * 5.最终close方法，回收MemorySegment的内存
 */
public class FileChannelInputView extends AbstractPagedInputView {
	
	private final BlockChannelReader<MemorySegment> reader;
	
	private final MemoryManager memManager;
	
	private final List<MemorySegment> memory;
	
	private final int sizeOfLastBlock;
	
	private int numRequestsRemaining;
	
	private int numBlocksRemaining;
	
	// --------------------------------------------------------------------------------------------

	/**
	 *
	 * @param reader 如何读取file文件内容
	 * @param memManager 用于释放segment内存
	 * @param memory 用于读取文件内容的segment集合，预先申请一批segment代用
	 * @param sizeOfLastBlock
	 * @throws IOException
	 */
	public FileChannelInputView(BlockChannelReader<MemorySegment> reader, MemoryManager memManager, List<MemorySegment> memory, int sizeOfLastBlock) throws IOException {
		super(0);
		
		checkNotNull(reader);
		checkNotNull(memManager);
		checkNotNull(memory);
		checkArgument(!reader.isClosed());
		checkArgument(memory.size() > 0);
		
		this.reader = reader;
		this.memManager = memManager;
		this.memory = memory;
		this.sizeOfLastBlock = sizeOfLastBlock;
		
		try {
			final long channelLength = reader.getSize();//文件总大小
			final int segmentSize = memManager.getPageSize();//每一个segment的固定大小
			
			this.numBlocksRemaining = MathUtils.checkedDownCast(channelLength / segmentSize);//多少个segment
			if (channelLength % segmentSize != 0) {//最后一个数据块没装满segment
				this.numBlocksRemaining++;
			}
			
			this.numRequestsRemaining = numBlocksRemaining;
			
			for (int i = 0; i < memory.size(); i++) {
				sendReadRequest(memory.get(i));//先读取N个segment数据
			}
			
			advance();
		}
		catch (IOException e) {
			memManager.release(memory);
			throw e;
		}
	}
	
	public void close() throws IOException {
		close(false);
	}
	
	public void closeAndDelete() throws IOException {
		close(true);
	}
	
	private void close(boolean deleteFile) throws IOException {
		try {
			clear();
			if (deleteFile) {
				reader.closeAndDelete();
			} else {
				reader.close();
			}
		} finally {
			synchronized (memory) {
				memManager.release(memory);//释放内存
				memory.clear();
			}
		}
	}

	//填充参数segment数据,返回上一个填充好的segment
	@Override
	protected MemorySegment nextSegment(MemorySegment current) throws IOException {
		// check for end-of-stream
		if (numBlocksRemaining <= 0) {
			reader.close();
			throw new EOFException();
		}
		
		// send a request first. if we have only a single segment, this same segment will be the one obtained in the next lines
		if (current != null) {
			sendReadRequest(current);
		}
		
		// get the next segment
		numBlocksRemaining--;
		return reader.getNextReturnedBlock();//阻塞,读取下一个有数据的segment
	}
	
	@Override
	protected int getLimitForSegment(MemorySegment segment) {
		return numBlocksRemaining > 0 ? segment.size() : sizeOfLastBlock;
	}
	
	private void sendReadRequest(MemorySegment seg) throws IOException {
		if (numRequestsRemaining > 0) {
			reader.readBlock(seg);
			numRequestsRemaining--;
		} else {
			memManager.release(seg);
		}
	}
}
