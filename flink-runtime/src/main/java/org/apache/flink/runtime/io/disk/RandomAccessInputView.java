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

package org.apache.flink.runtime.io.disk;

import java.io.EOFException;
import java.util.ArrayList;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.SeekableDataInputView;
import org.apache.flink.runtime.memory.AbstractPagedInputView;
import org.apache.flink.util.MathUtils;


//外界只需要正常调用read方法即可，内部透明，将数据按照顺序从MemorySegment[] segments中读取出来，直到得到结尾为止
public class RandomAccessInputView extends AbstractPagedInputView implements SeekableDataInputView {
	
	private final ArrayList<MemorySegment> segments;//相当于数据库
	
	private int currentSegmentIndex;//当前该读取第几个segment内容了
	
	private final int segmentSizeBits;
	
	private final int segmentSizeMask;
	
	private final int segmentSize;//每一个segment大小
	
	private final int limitInLastSegment;//最后一个segment真实存储数据的大小，因为可能segment没存满
	
	public RandomAccessInputView(ArrayList<MemorySegment> segments, int segmentSize)
	{
		this(segments, segmentSize, segmentSize);
	}

	/**
	 *
	 * @param segments 存储数据的源头
	 * @param segmentSize 每一个segment大小
	 * @param limitInLastSegment 最后一个segment真实存储数据的大小，因为可能segment没存满
	 */
	public RandomAccessInputView(ArrayList<MemorySegment> segments, int segmentSize, int limitInLastSegment) {
		super(segments.get(0), segments.size() > 1 ? segmentSize : limitInLastSegment, 0);
		this.segments = segments;
		this.currentSegmentIndex = 0;
		this.segmentSize = segmentSize;
		this.segmentSizeBits = MathUtils.log2strict(segmentSize);
		this.segmentSizeMask = segmentSize - 1;
		this.limitInLastSegment = limitInLastSegment;
	}


	//设置该读取哪个segment的那个位置了
	@Override
	public void setReadPosition(long position) {
		final int bufferNum = (int) (position >>> this.segmentSizeBits);
		final int offset = (int) (position & this.segmentSizeMask);
		
		this.currentSegmentIndex = bufferNum;
		seekInput(this.segments.get(bufferNum), offset, bufferNum < this.segments.size() - 1 ? this.segmentSize : this.limitInLastSegment);
	}

	//全局看,已经读取了多少个字节了
	public long getReadPosition() {
		return (((long) currentSegmentIndex) << segmentSizeBits) + getCurrentPositionInSegment();
	}


	//返回下一个待读取的segment
	@Override
	protected MemorySegment nextSegment(MemorySegment current) throws EOFException {
		if (++this.currentSegmentIndex < this.segments.size()) {
			return this.segments.get(this.currentSegmentIndex);
		} else {
			throw new EOFException();
		}
	}

    //当前segment的大小
	@Override
	protected int getLimitForSegment(MemorySegment segment) {
		return this.currentSegmentIndex == this.segments.size() - 1 ? this.limitInLastSegment : this.segmentSize;
	}
}
