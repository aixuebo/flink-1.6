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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Helper class to read {@link Buffer}s from files into objects.
 * 对读取文件的流加一层buffer缓存
 *
 * 缓冲流原理:int(标识是event还是buffer),int(表示接下来存储的size).每次向缓冲区填充size个字节
 */
public class BufferFileChannelReader {
	private final ByteBuffer header = ByteBuffer.allocateDirect(8);
	private final FileChannel fileChannel;//流本身

	BufferFileChannelReader(FileChannel fileChannel) {
		this.fileChannel = fileChannel;
	}

	/**
	 * Reads data from the object's file channel into the given buffer.
	 *
	 * @param buffer the buffer to read into
	 *
	 * @return whether the end of the file has been reached (<tt>true</tt>) or not (<tt>false</tt>) true表示不能继续读取数据了,即流没有数据了
	 * 读取数据,存储到参数buffer中
	 *
	 * 每次知读取一个buffer数据,填充到参数buffer中
	 */
	public boolean readBufferFromFileChannel(Buffer buffer) throws IOException {
		checkArgument(fileChannel.size() - fileChannel.position() > 0);//确保流有内容

		// Read header
		header.clear();
		fileChannel.read(header);
		header.flip();

		final boolean isBuffer = header.getInt() == 1;
		final int size = header.getInt();//接下来流要读取的数据size

		if (size > buffer.getMaxCapacity()) {//要读取的size 比 buffer还大,说明buffer容纳不了
			throw new IllegalStateException("Buffer is too small for data: " + buffer.getMaxCapacity() + " bytes available, but " + size + " needed. This is most likely due to an serialized event, which is larger than the buffer size.");
		}
		checkArgument(buffer.getSize() == 0, "Buffer not empty");//确保缓冲区buffer目前是空

		fileChannel.read(buffer.getNioBuffer(0, size));//填充buffer内容
		buffer.setSize(size);

		if (!isBuffer) {//表示是event
			buffer.tagAsEvent();
		}

		return fileChannel.size() - fileChannel.position() == 0;
	}
}
