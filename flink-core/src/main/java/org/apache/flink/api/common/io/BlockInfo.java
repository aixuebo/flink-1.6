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

package org.apache.flink.api.common.io;

import java.io.IOException;

import org.apache.flink.annotation.Public;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

/**
 * A block of 24 bytes written at the <i>end</i> of a block in a binary file, and containing
 * i) the number of records in the block, ii) the accumulated number of records, and
 * iii) the offset of the first record in the block.
 * 代表数据块如何存储,虽然数据块内存储的是字节数组数据，但数据块的最后24位置是存的数据块元数据信息
 * */
@Public
public class BlockInfo implements IOReadableWritable {

	private long recordCount;//数据块中有多少条数据

	private long accumulatedRecordCount;//全部数据块中包含多少条数据

	private long firstRecordStart;//第一条记录在该数据块中的偏移量

	//数据块元数据占用字节数
	public int getInfoSize() {
		return 8 + 8 + 8;
	}

	/**
	 * Returns the firstRecordStart.
	 * 
	 * @return the firstRecordStart
	 */
	public long getFirstRecordStart() {
		return this.firstRecordStart;
	}

	/**
	 * Sets the firstRecordStart to the specified value.
	 * 
	 * @param firstRecordStart
	 *        the firstRecordStart to set
	 */
	public void setFirstRecordStart(long firstRecordStart) {
		this.firstRecordStart = firstRecordStart;
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		out.writeLong(this.recordCount);
		out.writeLong(this.accumulatedRecordCount);
		out.writeLong(this.firstRecordStart);
	}

	@Override
	public void read(DataInputView in) throws IOException {
		this.recordCount = in.readLong();
		this.accumulatedRecordCount = in.readLong();
		this.firstRecordStart = in.readLong();
	}

	/**
	 * Returns the recordCount.
	 * 
	 * @return the recordCount
	 */
	public long getRecordCount() {
		return this.recordCount;
	}

	/**
	 * Returns the accumulated record count.
	 * 
	 * @return the accumulated record count
	 */
	public long getAccumulatedRecordCount() {
		return this.accumulatedRecordCount;
	}

	/**
	 * Sets the accumulatedRecordCount to the specified value.
	 * 
	 * @param accumulatedRecordCount
	 *        the accumulatedRecordCount to set
	 */
	public void setAccumulatedRecordCount(long accumulatedRecordCount) {
		this.accumulatedRecordCount = accumulatedRecordCount;
	}

	/**
	 * Sets the recordCount to the specified value.
	 * 
	 * @param recordCount
	 *        the recordCount to set
	 */
	public void setRecordCount(long recordCount) {
		this.recordCount = recordCount;
	}
}
