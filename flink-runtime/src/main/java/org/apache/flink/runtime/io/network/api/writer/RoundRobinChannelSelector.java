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

package org.apache.flink.runtime.io.network.api.writer;

import org.apache.flink.core.io.IOReadableWritable;

/**
 * This is the default implementation of the {@link ChannelSelector} interface. It represents a simple round-robin
 * strategy, i.e. regardless of the record every attached exactly one output channel is selected at a time.

 * @param <T> 必须支持可序列化
 *        the type of record which is sent through the attached output gate
 * 轮训将record输出到某一个partition中
 */
public class RoundRobinChannelSelector<T extends IOReadableWritable> implements ChannelSelector<T> {

	/**
	 * Stores the index of the channel to send the next record to.
	 */
	private final int[] nextChannelToSendTo = new int[1];

	/**
	 * Constructs a new default channel selector.
	 */
	public RoundRobinChannelSelector() {
		this.nextChannelToSendTo[0] = 0;
	}

	@Override
	public int[] selectChannels(final T record, final int numberOfOutputChannels) {

		int newChannel = ++this.nextChannelToSendTo[0];//每次更新数组的值+1
		if (newChannel >= numberOfOutputChannels) {
			this.nextChannelToSendTo[0] = 0;
		}

		return this.nextChannelToSendTo;
	}
}
