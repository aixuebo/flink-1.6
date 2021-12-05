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

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.util.event.NotificationListener;
import org.apache.flink.util.FileUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A base class for readers and writers that accept read or write requests for whole blocks.
 * The request is delegated to an asynchronous I/O thread. After completion of the I/O request, the memory
 * segment of the block is added to a collection to be returned.
 * <p>
 * The asynchrony of the access makes it possible to implement read-ahead or write-behind types of I/O accesses.
 * 
 * @param <R> The type of request (e.g. <tt>ReadRequest</tt> or <tt>WriteRequest</tt> issued by this access to the I/O threads.
 *
 * R 表示什么类型的请求，比如读取Segment请求,即每次读取一个Segment内容。
 * T 表示针对R类型的请求,需要什么容器承接。
 */
public abstract class AsynchronousFileIOChannel<T, R extends IORequest> extends AbstractFileIOChannel {

	private final Object listenerLock = new Object();

	/**
	 * The lock that is used during closing to synchronize the thread that waits for all
	 * requests to be handled with the asynchronous I/O thread.
	 */
	protected final Object closeLock = new Object();

	/** A request queue for submitting asynchronous requests to the corresponding IO worker thread. */
	protected final RequestQueue<R> requestQueue;//存储request请求队列

	/** An atomic integer that counts the number of requests that we still wait for to return. 请求中的数量*/
	protected final AtomicInteger requestsNotReturned = new AtomicInteger(0);
	
	/** Handler for completed requests */
	protected final RequestDoneCallback<T> resultHandler;//回调函数

	/** An exception that was encountered by the asynchronous request handling thread. */
	protected volatile IOException exception;

	/** Flag marking this channel as closed */
	protected volatile boolean closed;

	private NotificationListener allRequestsProcessedListener;

	// --------------------------------------------------------------------------------------------

	/**
	 * Creates a new channel access to the path indicated by the given ID. The channel accepts buffers to be
	 * read/written and hands them to the asynchronous I/O thread. After being processed, the buffers
	 * are returned by adding the to the given queue.
	 *
	 * @param channelID    The id describing the path of the file that the channel accessed.文件
	 * @param requestQueue The queue that this channel hands its IO requests to.一个阻塞队列,存储读取动作的请求
	 * @param callback     The callback to be invoked when a request is done.当读取成功或者失败的时候,如何回调。当一个R执行完后，参与回调
	 * @param writeEnabled Flag describing whether the channel should be opened in read/write mode, rather
	 *                     than in read-only mode.是否可以向文件写入数据
	 * @throws IOException Thrown, if the channel could no be opened.
	 */
	protected AsynchronousFileIOChannel(FileIOChannel.ID channelID, RequestQueue<R> requestQueue, 
			RequestDoneCallback<T> callback, boolean writeEnabled) throws IOException
	{
		super(channelID, writeEnabled);

		this.requestQueue = checkNotNull(requestQueue);
		this.resultHandler = checkNotNull(callback);
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public boolean isClosed() {
		return this.closed;
	}

	/**
	 * Closes the channel and waits until all pending asynchronous requests are processed. The
	 * underlying <code>FileChannel</code> is closed even if an exception interrupts the closing.
	 *
	 * <p> <strong>Important:</strong> the {@link #isClosed()} method returns <code>true</code>
	 * immediately after this method has been called even when there are outstanding requests.
	 *
	 * @throws IOException Thrown, if an I/O exception occurred while waiting for the buffers, or if
	 *                     the closing was interrupted.
	 */
	@Override
	public void close() throws IOException {
		// atomically set the close flag
		synchronized (this.closeLock) {
			if (this.closed) {
				return;
			}
			this.closed = true;

			try {
				// wait until as many buffers have been returned as were written
				// only then is everything guaranteed to be consistent.
				while (this.requestsNotReturned.get() > 0) {//说明有请求尚未完成,等待全部完成后才能真实关闭
					try {
						// we add a timeout here, because it is not guaranteed that the
						// decrementing during buffer return and the check here are deadlock free.
						// the deadlock situation is however unlikely and caught by the timeout
						this.closeLock.wait(1000);
						checkErroneous();//有异常,则抛出,无异常,则不断循环
					}
					catch (InterruptedException iex) {
						throw new IOException("Closing of asynchronous file channel was interrupted.");
					}
				}

				// Additional check because we might have skipped the while loop
				checkErroneous();
			}
			finally {
				// close the file
				if (this.fileChannel.isOpen()) {
					this.fileChannel.close();
				}
			}
		}
	}
	
	/**
	 * This method waits for all pending asynchronous requests to return. When the
	 * last request has returned, the channel is closed and deleted.
	 * <p>
	 * Even if an exception interrupts the closing, such that not all request are handled,
	 * the underlying <tt>FileChannel</tt> is closed and deleted.
	 *
	 * @throws IOException Thrown, if an I/O exception occurred while waiting for the buffers, or if the closing was interrupted.
	 */
	@Override
	public void closeAndDelete() throws IOException {
		try {
			close();
		}
		finally {
			deleteChannel();
		}
	}

	/**
	 * Checks the exception state of this channel. The channel is erroneous, if one of its requests could not
	 * be processed correctly.
	 *
	 * @throws IOException Thrown, if the channel is erroneous. The thrown exception contains the original exception
	 *                     that defined the erroneous state as its cause.
	 */
	public final void checkErroneous() throws IOException {
		if (this.exception != null) {
			throw this.exception;
		}
	}

	/**
	 * Handles a processed <tt>Buffer</tt>. This method is invoked by the
	 * asynchronous IO worker threads upon completion of the IO request with the
	 * provided buffer and/or an exception that occurred while processing the request
	 * for that buffer.
	 *
	 * @param buffer The buffer to be processed.读取到的数据结果
	 * @param ex     The exception that occurred in the I/O threads when processing the buffer's request.读取过程中出现问题。
	 *
	 * 处理读取到的结果
	 */
	final protected void handleProcessedBuffer(T buffer, IOException ex) {
		if (buffer == null) {//说明没有读取到结果
			return;
		}

		// even if the callbacks throw an error, we need to maintain our bookkeeping
		try {
			if (ex != null && this.exception == null) {
				this.exception = ex;
				this.resultHandler.requestFailed(buffer, ex);//处理失败
			}
			else {
				this.resultHandler.requestSuccessful(buffer);//处理成功
			}
		}
		finally {
			NotificationListener listener = null;

			// Decrement the number of outstanding requests. If we are currently closing, notify the
			// waiters. If there is a listener, notify her as well.
			synchronized (this.closeLock) {
				if (this.requestsNotReturned.decrementAndGet() == 0) {
					if (this.closed) {
						this.closeLock.notifyAll();
					}

					synchronized (listenerLock) {
						listener = allRequestsProcessedListener;
						allRequestsProcessedListener = null;
					}
				}
			}

			if (listener != null) {
				listener.onNotification();
			}
		}
	}

	final protected void addRequest(R request) throws IOException {
		// check the error state of this channel
		checkErroneous();

		// write the current buffer and get the next one
		this.requestsNotReturned.incrementAndGet();//增加请求的数量

		if (this.closed || this.requestQueue.isClosed()) {//请求已经关闭了
			// if we found ourselves closed after the counter increment,
			// decrement the counter again and do not forward the request
			this.requestsNotReturned.decrementAndGet();

			final NotificationListener listener;

			synchronized (listenerLock) {
				listener = allRequestsProcessedListener;
				allRequestsProcessedListener = null;
			}

			if (listener != null) {
				listener.onNotification();
			}

			throw new IOException("I/O channel already closed. Could not fulfill: " + request);
		}

		this.requestQueue.add(request);//追加请求到队列
	}

	/**
	 * Registers a listener to be notified when all outstanding requests have been processed.
	 * 注册一个listener,当所有的请求都执行完成后,会发送通知
	 * <p> New requests can arrive right after the listener got notified. Therefore, it is not safe
	 * to assume that the number of outstanding requests is still zero after a notification unless
	 * there was a close right before the listener got called.
	 *
	 * <p> Returns <code>true</code>, if the registration was successful. A registration can fail,
	 * if there are no outstanding requests when trying to register a listener.
	 */
	protected boolean registerAllRequestsProcessedListener(NotificationListener listener) throws IOException {
		checkNotNull(listener);

		synchronized (listenerLock) {
			if (allRequestsProcessedListener == null) {
				// There was a race with the processing of the last outstanding request
				if (requestsNotReturned.get() == 0) {
					return false;
				}

				allRequestsProcessedListener = listener;

				return true;
			}
		}

		throw new IllegalStateException("Already subscribed.");
	}
}

//--------------------------------------------------------------------------------------------

/**
 * Read request that reads an entire memory segment from a block reader.
 * 读取数据,存储到MemorySegment中,即读取的是一整块的MemorySegment可以读取到的内容size
 * 即每次读取一个Segment大小的数据信息
 */
final class SegmentReadRequest implements ReadRequest {

	private final AsynchronousFileIOChannel<MemorySegment, ReadRequest> channel;

	private final MemorySegment segment;//读取数据内容,写入到MemorySegment中

	protected SegmentReadRequest(AsynchronousFileIOChannel<MemorySegment, ReadRequest> targetChannel, MemorySegment segment) {
		if (segment == null) {
			throw new NullPointerException("Illegal read request with null memory segment.");
		}
		
		this.channel = targetChannel;
		this.segment = segment;
	}

	@Override
	public void read() throws IOException {
		final FileChannel c = this.channel.fileChannel;
		if (c.size() - c.position() > 0) {//文件流有内容
			try {
				final ByteBuffer wrapper = this.segment.wrap(0, this.segment.size());
				this.channel.fileChannel.read(wrapper);//将数据填充到segment中
			}
			catch (NullPointerException npex) {
				throw new IOException("Memory segment has been released.");
			}
		}
	}

	@Override
	public void requestDone(IOException ioex) {
		this.channel.handleProcessedBuffer(this.segment, ioex);
	}
}

//--------------------------------------------------------------------------------------------

/**
 * Write request that writes an entire memory segment to the block writer.
 * 将MemorySegment的内容写到文件流中
 */
final class SegmentWriteRequest implements WriteRequest {

	private final AsynchronousFileIOChannel<MemorySegment, WriteRequest> channel;

	private final MemorySegment segment;

	protected SegmentWriteRequest(AsynchronousFileIOChannel<MemorySegment, WriteRequest> targetChannel, MemorySegment segment) {
		this.channel = targetChannel;
		this.segment = segment;
	}

	//将segment的内容输出到channel中
	@Override
	public void write() throws IOException {
		try {
			//this.channel.fileChannel 对应要写入的文件流
			FileUtils.writeCompletely(this.channel.fileChannel, this.segment.wrap(0, this.segment.size()));
		}
		catch (NullPointerException npex) {
			throw new IOException("Memory segment has been released.");
		}
	}

	@Override
	public void requestDone(IOException ioex) {
		this.channel.handleProcessedBuffer(this.segment, ioex);
	}
}

//将buffer内容输出到文件流中
final class BufferWriteRequest implements WriteRequest {

	private final AsynchronousFileIOChannel<Buffer, WriteRequest> channel;

	private final Buffer buffer;

	protected BufferWriteRequest(AsynchronousFileIOChannel<Buffer, WriteRequest> targetChannel, Buffer buffer) {
		this.channel = checkNotNull(targetChannel);
		this.buffer = checkNotNull(buffer);
	}

	@Override
	public void write() throws IOException {
		ByteBuffer nioBufferReadable = buffer.getNioBufferReadable();//将buffer转换成可读的buffer

		final ByteBuffer header = ByteBuffer.allocateDirect(8);

		header.putInt(buffer.isBuffer() ? 1 : 0);//buffer类型
		header.putInt(nioBufferReadable.remaining());//buffer的size
		header.flip();

		FileUtils.writeCompletely(channel.fileChannel, header);//输出8个字节头文件
		FileUtils.writeCompletely(channel.fileChannel, nioBufferReadable);//输出buffer具体内容
	}

	@Override
	public void requestDone(IOException error) {
		channel.handleProcessedBuffer(buffer, error);
	}
}

//读取一个buffer内容
final class BufferReadRequest implements ReadRequest {

	private final AsynchronousFileIOChannel<Buffer, ReadRequest> channel;

	private final Buffer buffer;

	private final AtomicBoolean hasReachedEndOfFile;//true表示已经读取到结尾

	protected BufferReadRequest(AsynchronousFileIOChannel<Buffer, ReadRequest> targetChannel, Buffer buffer, AtomicBoolean hasReachedEndOfFile) {
		this.channel = targetChannel;
		this.buffer = buffer;
		this.hasReachedEndOfFile = hasReachedEndOfFile;
	}

	@Override
	public void read() throws IOException {

		final FileChannel fileChannel = channel.fileChannel;

		if (fileChannel.size() - fileChannel.position() > 0) {//说明有文件还可以继续读
			BufferFileChannelReader reader = new BufferFileChannelReader(fileChannel);
			hasReachedEndOfFile.set(reader.readBufferFromFileChannel(buffer));//向buffer中添加读取到的内容,并且判断是否读取完数据
		}
		else {
			hasReachedEndOfFile.set(true);//说明没有文件可以读了
		}
	}

	@Override
	public void requestDone(IOException error) {
		channel.handleProcessedBuffer(buffer, error);
	}
}

//读取内容到FileSegment --- 读取一个buffer数据的元信息，组装成FileSegment对象返回。
final class FileSegmentReadRequest implements ReadRequest {

	private final AsynchronousFileIOChannel<FileSegment, ReadRequest> channel;

	private final AtomicBoolean hasReachedEndOfFile;

	private FileSegment fileSegment;

	protected FileSegmentReadRequest(AsynchronousFileIOChannel<FileSegment, ReadRequest> targetChannel, AtomicBoolean hasReachedEndOfFile) {
		this.channel = targetChannel;
		this.hasReachedEndOfFile = hasReachedEndOfFile;
	}

	@Override
	public void read() throws IOException {

		final FileChannel fileChannel = channel.fileChannel;

		if (fileChannel.size() - fileChannel.position() > 0) {//说明有内容
			final ByteBuffer header = ByteBuffer.allocateDirect(8);

			fileChannel.read(header);
			header.flip();

			final long position = fileChannel.position();

			final boolean isBuffer = header.getInt() == 1;
			final int length = header.getInt();

			fileSegment = new FileSegment(fileChannel, position, length, isBuffer);

			// Skip the binary data
			fileChannel.position(position + length);

			hasReachedEndOfFile.set(fileChannel.size() - fileChannel.position() == 0);//是否读取完
		}
		else {
			hasReachedEndOfFile.set(true);
		}
	}

	@Override
	public void requestDone(IOException error) {
		channel.handleProcessedBuffer(fileSegment, error);
	}
}

/**
 * Request that seeks the underlying file channel to the given position.
 * 跳跃请求,在执行read或者write时,都会跳跃到position位置
 */
final class SeekRequest implements ReadRequest, WriteRequest {

	private final AsynchronousFileIOChannel<?, ?> channel;
	private final long position;

	protected SeekRequest(AsynchronousFileIOChannel<?, ?> channel, long position) {
		this.channel = channel;
		this.position = position;
	}

	@Override
	public void requestDone(IOException ioex) {
	}

	//跳跃到该位置
	@Override
	public void read() throws IOException {
		this.channel.fileChannel.position(position);
	}

	//跳跃到该位置
	@Override
	public void write() throws IOException {
		this.channel.fileChannel.position(position);
	}
}
