/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.core.memory;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.io.InputStream;

/**
 * Un-synchronized stream similar to Java's ByteArrayInputStream that also exposes the current position.
 * 字节数组作为数据源
 */
@Internal
public class ByteArrayInputStreamWithPos extends InputStream {

	private static final byte[] EMPTY = new byte[0];

	protected byte[] buffer;//缓存字节内容
	protected int position;//当前处理的位置
	protected int count;//buffer的可操作性的end结束位置
	protected int mark = 0;//可reset的position位置

	public ByteArrayInputStreamWithPos() {
		this(EMPTY);
	}

	public ByteArrayInputStreamWithPos(byte[] buffer) {
		this(buffer, 0, buffer.length);
	}

	public ByteArrayInputStreamWithPos(byte[] buffer, int offset, int length) {
		setBuffer(buffer, offset, length);
	}

	//读取一个字节,返回字节内容
	@Override
	public int read() {
		return (position < count) ? 0xFF & (buffer[position++]) : -1;
	}

	//从缓存中读取len个字节内容,存储到参数b的off之后
	//返回读取了多少个字节,读取的内容在参数b中
	@Override
	public int read(byte[] b, int off, int len) {
		Preconditions.checkNotNull(b);

		if (off < 0 || len < 0 || len > b.length - off) {
			throw new IndexOutOfBoundsException();
		}

		if (position >= count) { //没有数据可以被读取了
			return -1; // signal EOF
		}

		int available = count - position; //可以读取的内容

		if (len > available) { //只能读取多少个字节
			len = available;
		}

		if (len <= 0) {
			return 0;
		}

		System.arraycopy(buffer, position, b, off, len);//从buffer中读取数据到b中
		position += len;
		return len;//真正读取的字节长度
	}

	@Override
	public long skip(long toSkip) {
		long remain = count - position;

		if (toSkip < remain) {
			remain = toSkip < 0 ? 0 : toSkip;
		}

		position += remain;
		return remain;
	}

	@Override
	public boolean markSupported() {
		return true;
	}

	//标注一个位置,用于后期reset
	@Override
	public void mark(int readAheadLimit) {
		mark = position;
	}

	@Override
	public void reset() {
		position = mark;
	}

	@Override
	public int available() {
		return count - position;
	}

	@Override
	public void close() throws IOException {
	}

	public int getPosition() {
		return position;
	}

	public void setPosition(int pos) {
		Preconditions.checkArgument(pos >= 0 && pos <= count, "Position out of bounds.");
		this.position = pos;
	}

	public void setBuffer(byte[] buffer, int offset, int length) {
		this.count = Math.min(buffer.length, offset + length);
		setPosition(offset);
		this.buffer = buffer;
		this.mark = offset;
	}
}
