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

import javax.annotation.Nonnull;

/**
 * Reusable adapter to {@link DataInputView} that operates on given byte-arrays.
 * 读取字节数组
 */
public class ByteArrayDataInputView extends DataInputViewStreamWrapper {

	@Nonnull
	private final ByteArrayInputStreamWithPos inStreamWithPos;//数据源  --- 会构建一个字节数组,刚开始是空的，然后不断向该数据设置值,然后不断读取被消费掉

	public ByteArrayDataInputView() {
		super(new ByteArrayInputStreamWithPos());//创建一个空的数据源
		this.inStreamWithPos = (ByteArrayInputStreamWithPos) in;
	}

	//为数据源设置内容，然后不断消费内容
	public ByteArrayDataInputView(@Nonnull byte[] buffer) {
		this(buffer, 0, buffer.length);
	}

	public ByteArrayDataInputView(@Nonnull byte[] buffer, int offset, int length) {
		this();
		setData(buffer, offset, length);//对空的数据源存放数据，后期会消费掉
	}

	public int getPosition() {
		return inStreamWithPos.getPosition();
	}

	public void setPosition(int pos) {
		inStreamWithPos.setPosition(pos);
	}

	//每次通过设置setData,然后慢慢读取数据
	public void setData(@Nonnull byte[] buffer, int offset, int length) {
		inStreamWithPos.setBuffer(buffer, offset, length);
	}
}
