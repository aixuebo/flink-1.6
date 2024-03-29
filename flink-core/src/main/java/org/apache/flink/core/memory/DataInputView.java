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

package org.apache.flink.core.memory;

import org.apache.flink.annotation.Public;

import java.io.DataInput;
import java.io.IOException;


/**
 * This interface defines a view over some memory that can be used to sequentially read the contents of the memory.
 * The view is typically backed by one or more {@link org.apache.flink.core.memory.MemorySegment}.
 * 如何读取数据
 * 如何读取二进制数据源，将字节数组转换成 boolean、int、string等形式
 */
@Public
public interface DataInputView extends DataInput {

	/**
	 * Skips {@code numBytes} bytes of memory. In contrast to the {@link #skipBytes(int)} method,
	 * this method always skips the desired number of bytes or throws an {@link java.io.EOFException}.
	 *
	 * @param numBytes The number of bytes to skip.准备跳过的字节数量
	 *
	 * @throws IOException Thrown, if any I/O related problem occurred such that the input could not
	 *                     be advanced to the desired position.
	 *  必须要跳过这些字节，如果字节数量不满足,则抛异常
	 *
	 *  与skipBytes对比,skipBytes方法可以跳过一些字节,也可以不跳过,不强求
	 */
	void skipBytesToRead(int numBytes) throws IOException;

	/**
	 * Reads up to {@code len} bytes of memory and stores it into {@code b} starting at offset {@code off}.
	 * It returns the number of read bytes or -1 if there is no more data left.
	 *
	 * @param b byte array to store the data to
	 * @param off offset into byte array
	 * @param len byte length to read
	 * @return the number of actually read bytes of -1 if there is no more data left
	 * @throws IOException
	 * 读取字节数组到参数中
	 */
	int read(byte[] b, int off, int len) throws IOException;

	/**
	 * Tries to fill the given byte array {@code b}. Returns the actually number of read bytes or -1 if there is no
	 * more data.
	 *
	 * @param b byte array to store the data to
	 * @return the number of read bytes or -1 if there is no more data left
	 * @throws IOException
	 * 读取字节数组到参数中
	 */
	int read(byte[] b) throws IOException;
}
