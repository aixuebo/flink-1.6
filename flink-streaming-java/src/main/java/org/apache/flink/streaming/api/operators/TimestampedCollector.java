/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;

/**
 * Wrapper around an {@link Output} for user functions that expect a {@link Collector}.
 * Before giving the {@link TimestampedCollector} to a user function you must set
 * the timestamp that should be attached to emitted elements. Most operators
 * would set the timestamp of the incoming
 * {@link org.apache.flink.streaming.runtime.streamrecord.StreamRecord} here.
 *
 * @param <T> The type of the elements that can be emitted.
 *
 * 比如flatMap、ProcessOperator,一个元素转换成多个元素，但要公用同一个时间戳信息,因此使用该类
 */
@Internal
public class TimestampedCollector<T> implements Collector<T> {

	private final Output<StreamRecord<T>> output;

	private final StreamRecord<T> reuse;//重复使用同一个时间戳

	/**
	 * Creates a new {@link TimestampedCollector} that wraps the given {@link Output}.
	 */
	public TimestampedCollector(Output<StreamRecord<T>> output) {
		this.output = output;
		this.reuse = new StreamRecord<T>(null);//重复使用
	}

	@Override
	public void collect(T record) {
		output.collect(reuse.replace(record));
	}

	//一个元素进来后,设置时间戳
	public void setTimestamp(StreamRecord<?> timestampBase) {
		if (timestampBase.hasTimestamp()) {//说明有时间戳
			reuse.setTimestamp(timestampBase.getTimestamp());
		} else {
			reuse.eraseTimestamp();//无时间戳
		}
	}

	public void setAbsoluteTimestamp(long timestamp) {
		reuse.setTimestamp(timestamp);
	}

	public void eraseTimestamp() {
		reuse.eraseTimestamp();
	}

	@Override
	public void close() {
		output.close();
	}
}
