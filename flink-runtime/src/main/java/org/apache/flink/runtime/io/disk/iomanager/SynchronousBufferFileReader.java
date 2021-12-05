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

import org.apache.flink.runtime.io.network.buffer.Buffer;

import java.io.IOException;

/**
 * A synchronous {@link BufferFileReader} implementation.
 *
 * <p> This currently bypasses the I/O manager as it is the only synchronous implementation, which
 * is currently in use.
 *
 * TODO Refactor I/O manager setup and refactor this into it
 * 读取文件 -- 追加一层buffer缓存 --- 如何读取文件
 */
public class SynchronousBufferFileReader extends SynchronousFileIOChannel implements BufferFileReader {

	private final BufferFileChannelReader reader;//缓冲区对象

	private boolean hasReachedEndOfFile;//是否达到文件结尾

	/**
	 *
	 * @param channelID  文件
	 * @param writeEnabled  对该文件是否有写操作
	 * @throws IOException
	 */
	public SynchronousBufferFileReader(ID channelID, boolean writeEnabled) throws IOException {
		super(channelID, writeEnabled);
		this.reader = new BufferFileChannelReader(fileChannel);//对文件流进行缓存
	}

	//读取数据到buffer参数中
	@Override
	public void readInto(Buffer buffer) throws IOException {
		if (fileChannel.size() - fileChannel.position() > 0) {//还有字节可以读取
			hasReachedEndOfFile = reader.readBufferFromFileChannel(buffer);//从缓冲区中读取数据到buffer中
		}
		else {
			buffer.recycleBuffer();//回收参数buffer
		}
	}

	//跳跃到该位置
	@Override
	public void seekToPosition(long position) throws IOException {
		fileChannel.position(position);
	}

	@Override
	public boolean hasReachedEndOfFile() {
		return hasReachedEndOfFile;
	}
}
