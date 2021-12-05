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

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.memory.AbstractPagedInputView;

/**
 * A {@link org.apache.flink.core.memory.DataInputView} that is backed by a
 * {@link BlockChannelReader}, making it effectively a data input
 * stream. The view reads it data in blocks from the underlying channel. The view can only read data that
 * has been written by a {@link ChannelWriterOutputView}, due to block formatting.
 * 该类读取的是通过ChannelWriterOutputView格式写入的文件，为了提高效率,每次以数据块的形式读取一个数据块
 *
 * 传入读取多少个数据块、如何读取数据块、读取数据块的并发内存集合。就可以读取数据了
 */
public class ChannelReaderInputView extends AbstractPagedInputView {
	
	protected final BlockChannelReader<MemorySegment> reader;		// the block reader that reads memory segments
	
	protected int numRequestsRemaining;				// the number of block requests remaining 还剩余需要读取多少个数据块
	
	private final int numSegments;					// the number of memory segment the view works with 有多少个并发读取数据块能力
	
	private final ArrayList<MemorySegment> freeMem;	// memory gathered once the work is done 用于收集已经读取成功的数据块
	
	private boolean inLastBlock;					// flag indicating whether the view is already in the last block ,true标识已经读取了最后一个数据块了,已无数据块可读
	
	private boolean closed;							// flag indicating whether the reader is closed 标识是否已经读取完成
	
	// --------------------------------------------------------------------------------------------

	/**
	 * Creates a new channel reader that reads from the given channel until the last block
	 * (as marked by a {@link ChannelWriterOutputView}) is found.
	 * 
	 * @param reader The reader that reads the data from disk back into memory.
	 * @param memory A list of memory segments that the reader uses for reading the data in. If the
	 *               list contains more than one segment, the reader will asynchronously pre-fetch
	 *               blocks ahead.
	 * @param waitForFirstBlock A flag indicating weather this constructor call should block
	 *                          until the first block has returned from the asynchronous I/O reader.
	 * 
	 * @throws IOException Thrown, if the read requests for the first blocks fail to be
	 *                     served by the reader.
	 */
	public ChannelReaderInputView(BlockChannelReader<MemorySegment> reader, List<MemorySegment> memory, boolean waitForFirstBlock)
	throws IOException
	{
		this(reader, memory, -1, waitForFirstBlock);
	}
	
	/**
	 * Creates a new channel reader that reads from the given channel, expecting a specified
	 * number of blocks in the channel.
	 * <p>
	 * WARNING: The reader will lock if the number of blocks given here is actually lower than
	 * the actual number of blocks in the channel.
	 * 
	 * @param reader The reader that reads the data from disk back into memory.
	 * @param memory A list of memory segments that the reader uses for reading the data in. If the
	 *               list contains more than one segment, the reader will asynchronously pre-fetch
	 *               blocks ahead.
	 * @param numBlocks The number of blocks this channel will read. If this value is
	 *                  given, the reader avoids issuing pre-fetch requests for blocks
	 *                  beyond the channel size.
	 * @param waitForFirstBlock A flag indicating weather this constructor call should block
	 *                          until the first block has returned from the asynchronous I/O reader.
	 * 
	 * @throws IOException Thrown, if the read requests for the first blocks fail to be
	 *                     served by the reader.
	 */
	public ChannelReaderInputView(BlockChannelReader<MemorySegment> reader, List<MemorySegment> memory,
														int numBlocks, boolean waitForFirstBlock)
	throws IOException
	{
		this(reader, memory, numBlocks, ChannelWriterOutputView.HEADER_LENGTH, waitForFirstBlock);
	}
		
	/**
	 * Non public constructor to allow subclasses to use this input view with different headers.
	 * <p>
	 * WARNING: The reader will lock if the number of blocks given here is actually lower than
	 * the actual number of blocks in the channel.
	 * 
	 * @param reader The reader that reads the data from disk back into memory. 如何读取数据到MemorySegment中
	 * @param memory A list of memory segments that the reader uses for reading the data in. If the
	 *               list contains more than one segment, the reader will asynchronously pre-fetch
	 *               blocks ahead.
	 * @param numBlocks The number of blocks this channel will read. If this value is
	 *                  given, the reader avoids issuing pre-fetch requests for blocks
	 *                  beyond the channel size.从文件中要读取多少个数据块。
	 * @param headerLen The length of the header assumed at the beginning of the block. Note that the
	 *                  {@link #nextSegment(org.apache.flink.core.memory.MemorySegment)} method assumes the default header length,
	 *                  so any subclass changing the header length should override that methods as well.头文件,确定每一个数据块的元数据信息。不属于数据本身。因此读取的时候要跳过这些字节。
	 * @param waitForFirstBlock A flag indicating weather this constructor call should block
	 *                          until the first block has returned from the asynchronous I/O reader.
	 * 
	 * @throws IOException
	 */
	ChannelReaderInputView(BlockChannelReader<MemorySegment> reader, List<MemorySegment> memory,
				int numBlocks, int headerLen, boolean waitForFirstBlock)
	throws IOException
	{
		super(headerLen);
		
		if (reader == null || memory == null) {
			throw new NullPointerException();
		}
		if (memory.isEmpty()) {
			throw new IllegalArgumentException("Empty list of memory segments given.");
		}
		if (numBlocks < 1 && numBlocks != -1) {
			throw new IllegalArgumentException("The number of blocks must be a positive number, or -1, if unknown.");
		}
		
		this.reader = reader;
		this.numRequestsRemaining = numBlocks;
		this.numSegments = memory.size();
		this.freeMem = new ArrayList<MemorySegment>(this.numSegments);

		//发送并发读取数据块
		for (int i = 0; i < memory.size(); i++) {
			sendReadRequest(memory.get(i));
		}
		
		if (waitForFirstBlock) {
			advance();
		}
	}
	
	public void waitForFirstBlock() throws IOException
	{
		if (getCurrentSegment() == null) {
			advance();
		}
	}
	
	public boolean isClosed() {
		return this.closed;
	}
	
	/**
	 * Closes this InputView, closing the underlying reader and returning all memory segments.
	 * 
	 * @return A list containing all memory segments originally supplied to this view.
	 * @throws IOException Thrown, if the underlying reader could not be properly closed.
	 */
	public List<MemorySegment> close() throws IOException {	
		if (this.closed) {
			throw new IllegalStateException("Already closed.");
		}
		this.closed = true;
		
		// re-collect all memory segments
		ArrayList<MemorySegment> list = this.freeMem;
		final MemorySegment current = getCurrentSegment();
		if (current != null) {
			list.add(current);
		}
		clear();

		// close the writer and gather all segments
		final LinkedBlockingQueue<MemorySegment> queue = this.reader.getReturnQueue();
		this.reader.close();

		while (list.size() < this.numSegments) {
			final MemorySegment m = queue.poll();
			if (m == null) {
				// we get null if the queue is empty. that should not be the case if the reader was properly closed.
				throw new RuntimeException("Bug in ChannelReaderInputView: MemorySegments lost.");
			}
			list.add(m);
		}
		return list;
	}
	
	// --------------------------------------------------------------------------------------------
	//                                        Utilities
	// --------------------------------------------------------------------------------------------

	/**
	 * Gets the next segment from the asynchronous block reader. If more requests are to be issued, the method
	 * first sends a new request with the current memory segment. If no more requests are pending, the method
	 * adds the segment to the readers return queue, which thereby effectively collects all memory segments.
	 * Secondly, the method fetches the next non-consumed segment
	 * returned by the reader. If no further segments are available, this method thrown an {@link EOFException}.
	 * 
	 * @param current The memory segment used for the next request.
	 * @return The memory segment to read from next.
	 * 
	 * @throws EOFException Thrown, if no further segments are available.
	 * @throws IOException Thrown, if an I/O error occurred while reading 
	 * @see AbstractPagedInputView#nextSegment(org.apache.flink.core.memory.MemorySegment)
	 */
	@Override
	protected MemorySegment nextSegment(MemorySegment current) throws IOException {
		// check if we are at our end
		if (this.inLastBlock) {
			throw new EOFException();
		}
				
		// send a request first. if we have only a single segment, this same segment will be the one obtained in
		// the next lines
		if (current != null) { //重复利用内存segment,再次发送请求填充数据块
			sendReadRequest(current);
		}
		
		// get the next segment 真正获取已经读取到的数据块
		final MemorySegment seg = this.reader.getNextReturnedBlock();
		
		// check the header
		if (seg.getShort(0) != ChannelWriterOutputView.HEADER_MAGIC_NUMBER) {
			throw new IOException("The current block does not belong to a ChannelWriterOutputView / " +
					"ChannelReaderInputView: Wrong magic number.");
		}
		if ( (seg.getShort(ChannelWriterOutputView.HEADER_FLAGS_OFFSET) & ChannelWriterOutputView.FLAG_LAST_BLOCK) != 0) {//标识是否是最后一个数据块
			// last block
			this.numRequestsRemaining = 0;
			this.inLastBlock = true;
		}
		
		return seg;
	}
	
	//存储数据块占用多少字节,即segment有效的size
	@Override
	protected int getLimitForSegment(MemorySegment segment) {
		return segment.getInt(ChannelWriterOutputView.HEAD_BLOCK_LENGTH_OFFSET);
	}
	
	/**
	 * Sends a new read requests, if further requests remain. Otherwise, this method adds the segment
	 * directly to the readers return queue.
	 * 
	 * @param seg The segment to use for the read request.
	 * @throws IOException Thrown, if the reader is in error.
	 */
	protected void sendReadRequest(MemorySegment seg) throws IOException {
		if (this.numRequestsRemaining != 0) {
			this.reader.readBlock(seg);//发送请求,读取数据块,内容填充到参数seg里
			if (this.numRequestsRemaining != -1) {
				this.numRequestsRemaining--;//剩余数据块内容
			}
		} else {
			// directly add it to the end of the return queue
			this.freeMem.add(seg);
		}
	}
}
