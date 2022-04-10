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

package org.apache.flink.runtime.state.memory;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

/**
 * A state handle that contains stream state in a byte array.
 *
 * 内存存储数据，说明数据量小,不需要存储到文件中
 */
public class ByteStreamStateHandle implements StreamStateHandle {

	private static final long serialVersionUID = -5280226231202517594L;

	/**
	 * The state data.
	 * 数据内容 -- 最终存储checkpoint的结果用字节数组存储
	 */
	private final byte[] data;

	/**
	 * A unique name of by which this state handle is identified and compared. Like a filename, all
	 * {@link ByteStreamStateHandle} with the exact same name must also have the exact same content in data.
	 * 自定义一个uuid
	 */
	private final String handleName;

	/**
	 * Creates a new ByteStreamStateHandle containing the given data.
	 */
	public ByteStreamStateHandle(String handleName, byte[] data) {
		this.handleName = Preconditions.checkNotNull(handleName);
		this.data = Preconditions.checkNotNull(data);
	}

	//打开输入流
	@Override
	public FSDataInputStream openInputStream() throws IOException {
		return new ByteStateHandleInputStream(data);
	}

	public byte[] getData() {
		return data;
	}

	public String getHandleName() {
		return handleName;
	}

	@Override
	public void discardState() {
	}

	//数据大小
	@Override
	public long getStateSize() {
		return data.length;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof ByteStreamStateHandle)) {
			return false;
		}

		ByteStreamStateHandle that = (ByteStreamStateHandle) o;
		return handleName.equals(that.handleName);
	}

	@Override
	public int hashCode() {
		return 31 * handleName.hashCode();
	}

	@Override
	public String toString() {
		return "ByteStreamStateHandle{" +
			"handleName='" + handleName + '\'' +
			", dataBytes=" + data.length +
			'}';
	}

	/**
	 * An input stream view on a byte array.
	 * 代表一个输入流,数据源是字节数组
	 */
	private static final class ByteStateHandleInputStream extends FSDataInputStream {

		private final byte[] data;//数据源
		private int index;//读取数据源的第几个位置

		public ByteStateHandleInputStream(byte[] data) {
			this.data = data;
		}

		//直接跳到第desired个字节位置
		@Override
		public void seek(long desired) throws IOException {
			if (desired >= 0 && desired <= data.length) {
				index = (int) desired;
			} else {
				throw new IOException("position out of bounds");
			}
		}

		@Override
		public long getPos() throws IOException {
			return index;
		}

		//读取一个字节
		@Override
		public int read() throws IOException {
			return index < data.length ? data[index++] & 0xFF : -1;
		}

		//读取数据到b中,最多读取len个长度
		@Override
		public int read(byte[] b, int off, int len) throws IOException {
			// note that any bounds checking on "byte[] b" happend anyways by the
			// System.arraycopy() call below, so we don't add extra checks here

			final int bytesLeft = data.length - index;//剩余字节数
			if (bytesLeft > 0) {
				final int bytesToCopy = Math.min(len, bytesLeft);
				System.arraycopy(data, index, b, off, bytesToCopy);
				index += bytesToCopy;
				return bytesToCopy;
			}
			else {
				return -1;
			}
		}
	}
}
