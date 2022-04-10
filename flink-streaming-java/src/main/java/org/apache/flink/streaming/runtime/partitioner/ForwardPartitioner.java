/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.partitioner;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * Partitioner that forwards elements only to the locally running downstream operation.
 *
 * @param <T> Type of the elements in the Stream
 *
 * 将记录输出到下游本地的operator实例。ForwardPartitioner分区器要求上下游算子并行度一样
 *
 * 代码只在本地运行,所以转发给本地的第0个节点，原因是StreamingJobGraphGenerator类中使用DistributionPattern.POINTWISE方式

 */
@Internal
public class ForwardPartitioner<T> extends StreamPartitioner<T> {
	private static final long serialVersionUID = 1L;

	private final int[] returnArray = new int[] {0};//输出到某一个分区中

	@Override
	public int[] selectChannels(SerializationDelegate<StreamRecord<T>> record, int numberOfOutputChannels) {
		return returnArray;
	}

	public StreamPartitioner<T> copy() {
		return this;
	}

	@Override
	public String toString() {
		return "FORWARD";
	}
}
