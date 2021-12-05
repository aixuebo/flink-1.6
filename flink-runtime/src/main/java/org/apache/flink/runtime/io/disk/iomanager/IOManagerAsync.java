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
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.util.ShutdownHookUtil;

import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A version of the {@link IOManager} that uses asynchronous I/O.
 * 1.在本地开N个目录,用于存储数据，以文件的形式存储。
 * 2.每一个目录有一个读线程 && 一个写线程。
 * 3.由于每一个目录下读写是单线程的，因此不存在并发写数据，因此在线程内持有一个队列，循环依次从队列中取request请求,执行读写操作。
 * 4.当close时,一些请求request尚未完成,则以request.requestDone(ioex)方式，通知上有任务执行失败。
 */
public class IOManagerAsync extends IOManager implements UncaughtExceptionHandler {
	
	/** The writer threads used for asynchronous block oriented channel writing. */
	private final WriterThread[] writers;//每一个root目录分配一个写的线程

	/** The reader threads used for asynchronous block oriented channel reading. */
	private final ReaderThread[] readers;//每一个root目录分配一个读的线程

	/** Flag to signify that the IOManager has been shut down already */
	private final AtomicBoolean isShutdown = new AtomicBoolean();

	/** Shutdown hook to make sure that the directories are removed on exit */
	private final Thread shutdownHook;//shutdown的hook线程

	
	// -------------------------------------------------------------------------
	//               Constructors / Destructors
	// -------------------------------------------------------------------------

	/**
	 * Constructs a new asynchronous I/O manger, writing files to the system 's temp directory.
	 */
	public IOManagerAsync() {
		this(EnvironmentInformation.getTemporaryFileDirectory());
	}
	
	/**
	 * Constructs a new asynchronous I/O manger, writing file to the given directory.
	 * 
	 * @param tempDir The directory to write temporary files to.
	 */
	public IOManagerAsync(String tempDir) {
		this(new String[] {tempDir});
	}

	/**
	 * Constructs a new asynchronous I/O manger, writing file round robin across the given directories.
	 * 
	 * @param tempDirs The directories to write temporary files to.
	 */
	public IOManagerAsync(String[] tempDirs) {
		super(tempDirs);
		
		// start a write worker thread for each directory
		this.writers = new WriterThread[tempDirs.length];//每一个目录是单线程的写入数据,因此只要保证接收的是有顺序即可,不能存在网络延迟导致的乱序即可
		for (int i = 0; i < this.writers.length; i++) {
			final WriterThread t = new WriterThread();
			this.writers[i] = t;
			t.setName("IOManager writer thread #" + (i + 1));
			t.setDaemon(true);
			t.setUncaughtExceptionHandler(this);
			t.start();
		}

		// start a reader worker thread for each directory
		this.readers = new ReaderThread[tempDirs.length];
		for (int i = 0; i < this.readers.length; i++) {
			final ReaderThread t = new ReaderThread();
			this.readers[i] = t;
			t.setName("IOManager reader thread #" + (i + 1));
			t.setDaemon(true);
			t.setUncaughtExceptionHandler(this);
			t.start();
		}

		// install a shutdown hook that makes sure the temp directories get deleted
		this.shutdownHook = ShutdownHookUtil.addShutdownHook(this::shutdown, getClass().getSimpleName(), LOG);
	}

	/**
	 * Close method. Shuts down the reader and writer threads immediately, not waiting for their
	 * pending requests to be served. This method waits until the threads have actually ceased their
	 * operation.
	 */
	@Override
	public void shutdown() {
		// mark shut down and exit if it already was shut down
		if (!isShutdown.compareAndSet(false, true)) {
			return;
		}

		// Remove shutdown hook to prevent resource leaks
		ShutdownHookUtil.removeShutdownHook(shutdownHook, getClass().getSimpleName(), LOG);

		try {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Shutting down I/O manager.");
			}

			// close writing and reading threads with best effort and log problems
			// first notify all to close, then wait until all are closed

			for (WriterThread wt : writers) {
				try {
					wt.shutdown();
				}
				catch (Throwable t) {
					LOG.error("Error while shutting down IO Manager writer thread.", t);
				}
			}
			for (ReaderThread rt : readers) {
				try {
					rt.shutdown();
				}
				catch (Throwable t) {
					LOG.error("Error while shutting down IO Manager reader thread.", t);
				}
			}
			try {
				for (WriterThread wt : writers) {
					wt.join();
				}
				for (ReaderThread rt : readers) {
					rt.join();
				}
			}
			catch (InterruptedException iex) {
				// ignore this on shutdown
			}
		}
		finally {
			// make sure we call the super implementation in any case and at the last point,
			// because this will clean up the I/O directories
			super.shutdown();
		}
	}
	
	/**
	 * Utility method to check whether the IO manager has been properly shut down. The IO manager is considered
	 * to be properly shut down when it is closed and its threads have ceased operation.
	 * 
	 * @return True, if the IO manager has properly shut down, false otherwise.
	 * true表示完美的完成的shutdown动作
	 */
	@Override
	public boolean isProperlyShutDown() {
		boolean readersShutDown = true;
		for (ReaderThread rt : readers) {
			readersShutDown &= rt.getState() == Thread.State.TERMINATED;
		}
		
		boolean writersShutDown = true;
		for (WriterThread wt : writers) {
			writersShutDown &= wt.getState() == Thread.State.TERMINATED;
		}
		
		return isShutdown.get() && readersShutDown && writersShutDown && super.isProperlyShutDown();
	}


	@Override
	public void uncaughtException(Thread t, Throwable e) {
		LOG.error("IO Thread '" + t.getName() + "' terminated due to an exception. Shutting down I/O Manager.", e);
		shutdown();
	}
	
	// ------------------------------------------------------------------------
	//                        Reader / Writer instantiations
	// ------------------------------------------------------------------------

	/**
	 * 向文件中写入MemorySegment格式的数据
	 * @param channelID The descriptor for the channel to write to.向该文件写入数据
	 * @param returnQueue The queue to put the written buffers into.存储已经成功写入的MemorySegment
	 * @return
	 * @throws IOException
	 * 比下面的方法,提供更多的选择,比如可以获取已经完成的数据块。
	 * 下面的方法只能保存一个数据块
	 */
	@Override
	public BlockChannelWriter<MemorySegment> createBlockChannelWriter(FileIOChannel.ID channelID,
								LinkedBlockingQueue<MemorySegment> returnQueue) throws IOException
	{
		checkState(!isShutdown.get(), "I/O-Manger is shut down.");
		return new AsynchronousBlockWriter(channelID, this.writers[channelID.getThreadNum()].requestQueue, returnQueue);
	}

	/**
	 * 向文件中写入MemorySegment格式的数据
	 * @param channelID The descriptor for the channel to write to.
	 * @param callback The callback to be called for 自定义回调函数
	 * @return
	 * @throws IOException
	 * 只能保存一个数据块，回调函数自定义处理保存成功与否的逻辑
	 */
	@Override
	public BlockChannelWriterWithCallback<MemorySegment> createBlockChannelWriter(FileIOChannel.ID channelID, RequestDoneCallback<MemorySegment> callback) throws IOException {
		checkState(!isShutdown.get(), "I/O-Manger is shut down.");
		return new AsynchronousBlockWriterWithCallback(channelID, this.writers[channelID.getThreadNum()].requestQueue, callback);
	}
	
	/**
	 * Creates a block channel reader that reads blocks from the given channel. The reader reads asynchronously,
	 * such that a read request is accepted, carried out at some (close) point in time, and the full segment
	 * is pushed to the given queue.
	 * 
	 * @param channelID The descriptor for the channel to write to.待读取的文件
	 * @param returnQueue The queue to put the full buffers into.返回已经读取完成的数据块
	 * @return A block channel reader that reads from the given channel.
	 * @throws IOException Thrown, if the channel for the reader could not be opened.
	 * 以MemorySegment的方式读取数据
	 */
	@Override
	public BlockChannelReader<MemorySegment> createBlockChannelReader(FileIOChannel.ID channelID,
										LinkedBlockingQueue<MemorySegment> returnQueue) throws IOException
	{
		checkState(!isShutdown.get(), "I/O-Manger is shut down.");
		return new AsynchronousBlockReader(channelID, this.readers[channelID.getThreadNum()].requestQueue, returnQueue);
	}

	/**
	 * 以Buffer的形式写入数据到channelID中
	 * @param channelID 要写入的文件
	 * @return
	 * @throws IOException
	 */
	@Override
	public BufferFileWriter createBufferFileWriter(FileIOChannel.ID channelID) throws IOException {
		checkState(!isShutdown.get(), "I/O-Manger is shut down.");

		return new AsynchronousBufferFileWriter(channelID, writers[channelID.getThreadNum()].requestQueue);
	}

	/**
	 * 以Buffer的形式读取数据
	 * @param channelID 要读取的文件
	 * @param callback 读取的数据如何处理回调函数
	 * @return
	 * @throws IOException
	 */
	@Override
	public BufferFileReader createBufferFileReader(FileIOChannel.ID channelID, RequestDoneCallback<Buffer> callback) throws IOException {
		checkState(!isShutdown.get(), "I/O-Manger is shut down.");

		return new AsynchronousBufferFileReader(channelID, readers[channelID.getThreadNum()].requestQueue, callback);
	}

	/**
	 * 以BufferFile的方式读取文件
	 * @param channelID 要读取的文件
	 * @param callback 读取的数据如何处理回调函数
	 * @return
	 * @throws IOException
	 */
	@Override
	public BufferFileSegmentReader createBufferFileSegmentReader(FileIOChannel.ID channelID, RequestDoneCallback<FileSegment> callback) throws IOException {
		checkState(!isShutdown.get(), "I/O-Manger is shut down.");

		return new AsynchronousBufferFileSegmentReader(channelID, readers[channelID.getThreadNum()].requestQueue, callback);
	}

	/**
	 * Creates a block channel reader that reads all blocks from the given channel directly in one bulk.
	 * The reader draws segments to read the blocks into from a supplied list, which must contain as many
	 * segments as the channel has blocks. After the reader is done, the list with the full segments can be 
	 * obtained from the reader.
	 * <p>
	 * If a channel is not to be read in one bulk, but in multiple smaller batches, a  
	 * {@link BlockChannelReader} should be used.
	 * 
	 * @param channelID The descriptor for the channel to write to.
	 * @param targetSegments The list to take the segments from into which to read the data.
	 * @param numBlocks The number of blocks in the channel to read.
	 * @return A block channel reader that reads from the given channel.
	 * @throws IOException Thrown, if the channel for the reader could not be opened.
	 */
	@Override
	public BulkBlockChannelReader createBulkBlockChannelReader(FileIOChannel.ID channelID,
			List<MemorySegment> targetSegments, int numBlocks) throws IOException
	{
		checkState(!isShutdown.get(), "I/O-Manger is shut down.");
		return new AsynchronousBulkBlockReader(channelID, this.readers[channelID.getThreadNum()].requestQueue, targetSegments, numBlocks);
	}
	
	// -------------------------------------------------------------------------
	//                             For Testing
	// -------------------------------------------------------------------------
	//获取文件第几个目录的请求队列
	RequestQueue<ReadRequest> getReadRequestQueue(FileIOChannel.ID channelID) {
		return this.readers[channelID.getThreadNum()].requestQueue;
	}

	//获取向第几个目录写数据的队列
	RequestQueue<WriteRequest> getWriteRequestQueue(FileIOChannel.ID channelID) {
		return this.writers[channelID.getThreadNum()].requestQueue;
	}

	// -------------------------------------------------------------------------
	//                           I/O Worker Threads
	// -------------------------------------------------------------------------
	
	/**
	 * A worker thread for asynchronous reads.
	 */
	private static final class ReaderThread extends Thread {
		
		protected final RequestQueue<ReadRequest> requestQueue;

		private volatile boolean alive;

		// ---------------------------------------------------------------------
		// Constructors / Destructors
		// ---------------------------------------------------------------------
		
		protected ReaderThread() {
			this.requestQueue = new RequestQueue<ReadRequest>();
			this.alive = true;
		}
		
		/**
		 * Shuts the thread down. This operation does not wait for all pending requests to be served, halts the thread
		 * immediately. All buffers of pending requests are handed back to their channel readers and an exception is
		 * reported to them, declaring their request queue as closed.
		 */
		protected void shutdown() {
			synchronized (this) {
				if (alive) {
					alive = false;
					requestQueue.close();
					interrupt();
				}

				try {
					join(1000);
				}
				catch (InterruptedException ignored) {}
				
				// notify all pending write requests that the thread has been shut down
				IOException ioex = new IOException("IO-Manager has been closed.");
					
				while (!this.requestQueue.isEmpty()) {
					ReadRequest request = this.requestQueue.poll();
					if (request != null) {
						try {
							request.requestDone(ioex);
						}
						catch (Throwable t) {
							IOManagerAsync.LOG.error("The handler of the request complete callback threw an exception"
									+ (t.getMessage() == null ? "." : ": " + t.getMessage()), t);
						}
					}
				}
			}
		}

		// ---------------------------------------------------------------------
		//                             Main loop
		// ---------------------------------------------------------------------

		@Override
		public void run() {
			
			while (alive) {
				
				// get the next buffer. ignore interrupts that are not due to a shutdown.
				ReadRequest request = null;
				while (alive && request == null) {
					try {
						request = this.requestQueue.take();
					}
					catch (InterruptedException e) {
						if (!this.alive) {
							return;
						} else {
							IOManagerAsync.LOG.warn(Thread.currentThread() + " was interrupted without shutdown.");
						}
					}
				}
				
				// remember any IO exception that occurs, so it can be reported to the writer
				IOException ioex = null;

				try {
					// read buffer from the specified channel
					request.read();
				}
				catch (IOException e) {
					ioex = e;
				}
				catch (Throwable t) {
					ioex = new IOException("The buffer could not be read: " + t.getMessage(), t);
					IOManagerAsync.LOG.error("I/O reading thread encountered an error" + (t.getMessage() == null ? "." : ": " + t.getMessage()), t);
				}

				// invoke the processed buffer handler of the request issuing reader object
				try {
					request.requestDone(ioex);
				}
				catch (Throwable t) {
					IOManagerAsync.LOG.error("The handler of the request-complete-callback threw an exception" + (t.getMessage() == null ? "." : ": " + t.getMessage()), t);
				}
			} // end while alive
		}
		
	} // end reading thread
	
	/**
	 * A worker thread that asynchronously writes the buffers to disk.
	 * 异步的将数据写入到磁盘
	 */
	private static final class WriterThread extends Thread {
		
		protected final RequestQueue<WriteRequest> requestQueue;//接收向该磁盘写入数据的请求

		private volatile boolean alive;

		// ---------------------------------------------------------------------
		// Constructors / Destructors
		// ---------------------------------------------------------------------

		protected WriterThread() {
			this.requestQueue = new RequestQueue<WriteRequest>();
			this.alive = true;
		}

		/**
		 * Shuts the thread down. This operation does not wait for all pending requests to be served, halts the thread
		 * immediately. All buffers of pending requests are handed back to their channel writers and an exception is
		 * reported to them, declaring their request queue as closed.
		 */
		protected void shutdown() {
			synchronized (this) {
				if (alive) {
					alive = false;
					requestQueue.close();
					interrupt();
				}

				try {
					join(1000);
				}
				catch (InterruptedException ignored) {}
				
				// notify all pending write requests that the thread has been shut down
				IOException ioex = new IOException("IO-Manager has been closed.");
					
				while (!this.requestQueue.isEmpty()) {
					WriteRequest request = this.requestQueue.poll();
					if (request != null) {
						try {
							request.requestDone(ioex);
						}
						catch (Throwable t) {
							IOManagerAsync.LOG.error("The handler of the request complete callback threw an exception"
									+ (t.getMessage() == null ? "." : ": " + t.getMessage()), t);
						}
					}
				}
			}
		}

		// ---------------------------------------------------------------------
		// Main loop
		// ---------------------------------------------------------------------

		@Override
		public void run() {
			
			while (this.alive) {
				
				WriteRequest request = null;
				
				// get the next buffer. ignore interrupts that are not due to a shutdown.
				while (alive && request == null) {
					try {
						request = requestQueue.take();
					}
					catch (InterruptedException e) {
						if (!this.alive) {
							return;
						} else {
							IOManagerAsync.LOG.warn(Thread.currentThread() + " was interrupted without shutdown.");
						}
					}
				}
				
				// remember any IO exception that occurs, so it can be reported to the writer
				IOException ioex = null;
				
				try {
					// write buffer to the specified channel
					request.write();
				}
				catch (IOException e) {
					ioex = e;
				}
				catch (Throwable t) {
					ioex = new IOException("The buffer could not be written: " + t.getMessage(), t);
					IOManagerAsync.LOG.error("I/O writing thread encountered an error" + (t.getMessage() == null ? "." : ": " + t.getMessage()), t);
				}

				// invoke the processed buffer handler of the request issuing writer object
				try {
					request.requestDone(ioex);
				}
				catch (Throwable t) {
					IOManagerAsync.LOG.error("The handler of the request-complete-callback threw an exception" + (t.getMessage() == null ? "." : ": " + t.getMessage()), t);
				}
			} // end while alive
		}
		
	}; // end writer thread
}
