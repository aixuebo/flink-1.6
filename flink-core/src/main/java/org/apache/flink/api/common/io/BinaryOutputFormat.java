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

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.flink.annotation.Public;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

//将对象T序列化成字节数组,输出到文本文件中
@Public
public abstract class BinaryOutputFormat<T> extends FileOutputFormat<T> {
	
	private static final long serialVersionUID = 1L;
	
	/** The config parameter which defines the fixed length of a record. */
	public static final String BLOCK_SIZE_PARAMETER_KEY = "output.block_size";//数据块大小

	public static final long NATIVE_BLOCK_SIZE = Long.MIN_VALUE;

	/** The block size to use. 数据块大小*/
	private long blockSize = NATIVE_BLOCK_SIZE;

	private transient BlockBasedOutput blockBasedOutput;//对输出流进一步包装,每隔多少个数据块进行拆分成一个文件
	
	private transient DataOutputViewStreamWrapper outView;//输出流


	@Override
	public void close() throws IOException {
		try {
			DataOutputViewStreamWrapper o = this.outView;
			if (o != null) {
				o.close();
			}
		}
		finally {
			super.close();
		}
	}
	
	protected void complementBlockInfo(BlockInfo blockInfo) {}

	@Override
	public void configure(Configuration parameters) {
		super.configure(parameters);

		// read own parameters 初始化数据块大小
		this.blockSize = parameters.getLong(BLOCK_SIZE_PARAMETER_KEY, NATIVE_BLOCK_SIZE);
		if (this.blockSize < 1 && this.blockSize != NATIVE_BLOCK_SIZE) {
			throw new IllegalArgumentException("The block size parameter must be set and larger than 0.");
		}
		if (this.blockSize > Integer.MAX_VALUE) {
			throw new UnsupportedOperationException("Currently only block size up to Integer.MAX_VALUE are supported");
		}
	}

	protected BlockInfo createBlockInfo() {
		return new BlockInfo();
	}

	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
		super.open(taskNumber, numTasks);

		final long blockSize = this.blockSize == NATIVE_BLOCK_SIZE ?
			this.outputFilePath.getFileSystem().getDefaultBlockSize() : this.blockSize;

		this.blockBasedOutput = new BlockBasedOutput(this.stream, (int) blockSize);//对输出流进一步包装,每隔多少个数据块进行拆分成一个文件
		this.outView = new DataOutputViewStreamWrapper(this.blockBasedOutput);
	}

	//对象序列化成字节数组
	protected abstract void serialize(T record, DataOutputView dataOutput) throws IOException;

	@Override
	public void writeRecord(T record) throws IOException {
		this.blockBasedOutput.startRecord();
		this.serialize(record, outView);//输出序列化后的字节数组
	}

	/**
	 * Writes a block info at the end of the blocks.<br>
	 * Current implementation uses only int and not long.
	 * 
	 */
	protected class BlockBasedOutput extends FilterOutputStream {

		private static final int NO_RECORD = -1;

		private final int maxPayloadSize;//刨除数据块元数据字节数,数据块还能装多少字节数

		private int blockPos;//数据块的指针位置

		private int blockCount, totalCount; //数据块内总数据条数、输出的总数据条数

		private long firstRecordStartPos = NO_RECORD;//数据块中第一条数据在该数据块的偏移量，数据块中 该值之前的数据为上一条的数据

		private BlockInfo blockInfo = BinaryOutputFormat.this.createBlockInfo();

		private DataOutputView headerStream;

		public BlockBasedOutput(OutputStream out, int blockSize) {
			super(out);
			this.headerStream = new DataOutputViewStreamWrapper(out);
			this.maxPayloadSize = blockSize - this.blockInfo.getInfoSize();
		}

		@Override
		public void close() throws IOException {
			if (this.blockPos > 0) {
				this.writeInfo();
			}
			super.flush();
			super.close();
		}

		public void startRecord() {
			if (this.firstRecordStartPos == NO_RECORD) {//只在第一条数据的时候,更新该属性值
				this.firstRecordStartPos = this.blockPos;
			}
			this.blockCount++;
			this.totalCount++;
		}

		@Override
		public void write(byte[] b) throws IOException {
			this.write(b, 0, b.length);
		}

		//将b的数据,输出到输出流中
		@Override
		public void write(byte[] b, int off, int len) throws IOException {

			for (int remainingLength = len, offset = off; remainingLength > 0;) {//将数据写入到多个数据块中
				int blockLen = Math.min(remainingLength, this.maxPayloadSize - this.blockPos);//后面参数,表示数据块还剩余多少字节
				this.out.write(b, offset, blockLen);

				this.blockPos += blockLen;//数据块的指针位置
				if (this.blockPos >= this.maxPayloadSize) {//说明达到了一个数据块位置
					this.writeInfo();//写入一个数据块
				}
				remainingLength -= blockLen;
				offset += blockLen;
			}
		}

		@Override
		public void write(int b) throws IOException {
			super.write(b);
			if (++this.blockPos >= this.maxPayloadSize) {//达到数据块阈值了
				this.writeInfo();//创建新的数据块
			}
		}

		private void writeInfo() throws IOException {
			this.blockInfo.setRecordCount(this.blockCount);//数据块内数据条数
			this.blockInfo.setAccumulatedRecordCount(this.totalCount);//截止到当前输出的总条数
			this.blockInfo.setFirstRecordStart(this.firstRecordStartPos == NO_RECORD ? 0 : this.firstRecordStartPos);//第一条记录在该数据块中的偏移量
			BinaryOutputFormat.this.complementBlockInfo(this.blockInfo);
			this.blockInfo.write(this.headerStream);

			//重新设置数据块元数据
			this.blockPos = 0;
			this.blockCount = 0;
			this.firstRecordStartPos = NO_RECORD;
		}
	}
}
