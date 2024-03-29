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

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.flink.util.StringUtils;

/**
 * A Channel represents a collection of files that belong logically to the same resource. An example is a collection of
 * files that contain sorted runs of data from the same stream, that will later on be merged together.
 * 代表一个文件---对文件的读写流、文件size等信息、文件close、文件删除
 */
public interface FileIOChannel {
	
	/**
	 * Gets the channel ID of this I/O channel.
	 * 
	 * @return The channel ID.代表一个文件
	 */
	FileIOChannel.ID getChannelID();
	
	/**
	 * Gets the size (in bytes) of the file underlying the channel.
	 * 
	 * @return The size (in bytes) of the file underlying the channel.
	 * 文件size
	 */
	long getSize() throws IOException;
	
	/**
	 * Checks whether the channel has been closed.
	 * 
	 * @return True if the channel has been closed, false otherwise.
	 * 文件是否已经关闭了
	 */
	boolean isClosed();

	/**
	* Closes the channel. For asynchronous implementations, this method waits until all pending requests are
	* handled. Even if an exception interrupts the closing, the underlying <tt>FileChannel</tt> is closed.
	* 
	* @throws IOException Thrown, if an error occurred while waiting for pending requests.
	 * 真实去关闭文件
	*/
	void close() throws IOException;

	/**
	 * Deletes the file underlying this I/O channel.
	 *  
	 * @throws IllegalStateException Thrown, when the channel is still open.
	 * 删除文件
	 */
	void deleteChannel();
	
	/**
	* Closes the channel and deletes the underlying file.
	* For asynchronous implementations, this method waits until all pending requests are handled;
	* 
	* @throws IOException Thrown, if an error occurred while waiting for pending requests.
	 * 先关闭文件，再删除文件
	*/
	public void closeAndDelete() throws IOException;

	//返回文件的channel流 -- 用于读写该文件
	FileChannel getNioFileChannel();
	
	// --------------------------------------------------------------------------------------------
	// --------------------------------------------------------------------------------------------
	
	/**
	 * An ID identifying an underlying file channel.
	 * 产生一个随机文件名构成的文件 -- 文件名+线程序号,确定文件名
	 */
	public static class ID {
		
		private static final int RANDOM_BYTES_LENGTH = 16;
		
		private final File path;//随机产生的文件
		
		private final int threadNum;//该path是第几个目录的path

		protected ID(File path, int threadNum) {
			this.path = path;
			this.threadNum = threadNum;
		}

		protected ID(File basePath, int threadNum, Random random) {
			this.path = new File(basePath, randomString(random) + ".channel");//生产.channel文件
			this.threadNum = threadNum;
		}

		/**
		 * Returns the path to the underlying temporary file.
		 * @return The path to the underlying temporary file..
		 */
		public String getPath() {
			return path.getAbsolutePath();
		}

		/**
		 * Returns the path to the underlying temporary file as a File.
		 * @return The path to the underlying temporary file as a File.
		 */
		public File getPathFile() {
			return path;
		}
		
		int getThreadNum() {
			return this.threadNum;
		}

		@Override
		public boolean equals(Object obj) {
			if (obj instanceof ID) {
				ID other = (ID) obj;
				return this.path.equals(other.path) && this.threadNum == other.threadNum;
			} else {
				return false;
			}
		}
		
		@Override
		public int hashCode() {
			return path.hashCode();
		}
		
		@Override
		public String toString() {
			return path.getAbsolutePath();
		}
		
		private static String randomString(Random random) {
			byte[] bytes = new byte[RANDOM_BYTES_LENGTH];
			random.nextBytes(bytes);
			return StringUtils.byteToHexString(bytes);
		}
	}

	/**
	 * An enumerator for channels that logically belong together.
	 * 在paths[]这些root下,轮训创建.channel文件
	 */
	public static final class Enumerator {

		private static AtomicInteger globalCounter = new AtomicInteger();

		private final File[] paths;

		private final String namePrefix;//随机前缀

		private int localCounter;

		protected Enumerator(File[] basePaths, Random random) {
			this.paths = basePaths;
			this.namePrefix = ID.randomString(random);
			this.localCounter = 0;
		}

		public ID next() {
			// The local counter is used to increment file names while the global counter is used
			// for indexing the directory and associated read and write threads. This performs a
			// round-robin among all spilling operators and avoids I/O bunching.
			int threadNum = globalCounter.getAndIncrement() % paths.length;
			String filename = String.format("%s.%06d.channel", namePrefix, (localCounter++));
			return new ID(new File(paths[threadNum], filename), threadNum);
		}
	}
}
