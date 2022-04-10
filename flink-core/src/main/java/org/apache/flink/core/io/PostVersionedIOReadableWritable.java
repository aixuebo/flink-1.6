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

package org.apache.flink.core.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.util.Arrays;

/**
 * A {@link VersionedIOReadableWritable} which allows to differentiate whether the previous
 * data was versioned with a {@link VersionedIOReadableWritable}. This can be used if previously
 * written data was not versioned, and is to be migrated to a versioned format.
 *
 * 优先写入魔数据 + 版本数据 --- 子类实现写入具体对象的序列化数据。
 * 因此保证了继承该类的子类,不仅有对象序列化数据，还有魔+版本数据
 */
@Internal
public abstract class PostVersionedIOReadableWritable extends VersionedIOReadableWritable {

	/** NOTE: CANNOT CHANGE! 版本标识符 */
	private static final byte[] VERSIONED_IDENTIFIER = new byte[] {-15, -51, -123, -97};

	/**
	 * Read from the provided {@link DataInputView in}. A flag {@code wasVersioned} can be
	 * used to determine whether or not the data to read was previously written
	 * by a {@link VersionedIOReadableWritable}.
	 * 具体读取数据信息
	 */
	protected abstract void read(DataInputView in, boolean wasVersioned) throws IOException;

	@Override
	public void write(DataOutputView out) throws IOException {
		out.write(VERSIONED_IDENTIFIER);//写魔数据
		super.write(out);//写版本号数据
	}

	/**
	 * This read attempts to first identify if the input view contains the special
	 * {@link #VERSIONED_IDENTIFIER} by reading and buffering the first few bytes.
	 * If identified to be versioned, the usual version resolution read path
	 * in {@link VersionedIOReadableWritable#read(DataInputView)} is invoked.
	 * Otherwise, we "reset" the input stream by pushing back the read buffered bytes
	 * into the stream.
	 */
	public final void read(InputStream inputStream) throws IOException {
		byte[] tmp = new byte[VERSIONED_IDENTIFIER.length];
		inputStream.read(tmp);//读取模

		if (Arrays.equals(tmp, VERSIONED_IDENTIFIER)) {//确保模数据是相同的
			DataInputView inputView = new DataInputViewStreamWrapper(inputStream);

			super.read(inputView);//读取版本号
			read(inputView, true);
		} else {
			PushbackInputStream resetStream = new PushbackInputStream(inputStream, VERSIONED_IDENTIFIER.length);
			resetStream.unread(tmp);

			read(new DataInputViewStreamWrapper(resetStream), false);
		}
	}

	/**
	 * We do not support reading from a {@link DataInputView}, because it does not
	 * support pushing back already read bytes.
	 * 读取具体的内容
	 */
	@Override
	public final void read(DataInputView in) throws IOException {
		throw new UnsupportedOperationException("PostVersionedIOReadableWritable cannot read from a DataInputView.");
	}
}
