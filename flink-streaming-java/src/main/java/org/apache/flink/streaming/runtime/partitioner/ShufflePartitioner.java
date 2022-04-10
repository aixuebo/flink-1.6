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

import java.util.Random;

/**
 * Partitioner that distributes the data equally by selecting one output channel
 * randomly.
 *
 * @param <T>
 *            Type of the Tuple
 *  打混的方式输出到一个通道中 --- 缺点是下游接收到的数据不是按照key排序后接受的,似乎没什么价值,但可以把数据打散,解决数据倾斜问题
 */
@Internal
public class ShufflePartitioner<T> extends StreamPartitioner<T> {
	private static final long serialVersionUID = 1L;

	private Random random = new Random();

	private final int[] returnArray = new int[1];//输出到某一个通道

	@Override
	public int[] selectChannels(SerializationDelegate<StreamRecord<T>> record,
			int numberOfOutputChannels) {
		returnArray[0] = random.nextInt(numberOfOutputChannels);//随机生产一个通道
		return returnArray;
	}

	@Override
	public StreamPartitioner<T> copy() {
		return new ShufflePartitioner<T>();
	}

	@Override
	public String toString() {
		return "SHUFFLE";
	}
}
