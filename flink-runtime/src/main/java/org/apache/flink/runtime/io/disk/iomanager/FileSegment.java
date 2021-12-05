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

import java.nio.channels.FileChannel;

//标识一个读取的文件范围
//读取一个buffer数据的元信息，组装成FileSegment对象返回。
//目标是可以异步同时读取多个数据块
public class FileSegment {

	private final FileChannel fileChannel;//具体文件流句柄
	private final long position;//从该位置开始读数据
	private final int length;//需要读取的长度
	private final boolean isBuffer;//读取数据的类型,是buffer还是event

	public FileSegment(FileChannel fileChannel, long position, int length, boolean isBuffer) {
		this.fileChannel = fileChannel;
		this.position = position;
		this.length = length;
		this.isBuffer = isBuffer;
	}

	public FileChannel getFileChannel() {
		return fileChannel;
	}

	public long getPosition() {
		return position;
	}

	public int getLength() {
		return length;
	}

	public boolean isBuffer() {
		return isBuffer;
	}
}
