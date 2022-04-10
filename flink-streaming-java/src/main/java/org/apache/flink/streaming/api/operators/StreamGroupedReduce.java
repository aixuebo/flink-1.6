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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * A {@link StreamOperator} for executing a {@link ReduceFunction} on a
 * {@link org.apache.flink.streaming.api.datastream.KeyedStream}.
 *
 * 聚合函数,使用当前元素与merge的结果集进行计算
 * 每次都生产一个新的聚合后的值
 */
@Internal
public class StreamGroupedReduce<IN> extends AbstractUdfStreamOperator<IN, ReduceFunction<IN>>
		implements OneInputStreamOperator<IN, IN> {

	private static final long serialVersionUID = 1L;

	private static final String STATE_NAME = "_op_state";

	private transient ValueState<IN> values;

	private TypeSerializer<IN> serializer;

	public StreamGroupedReduce(ReduceFunction<IN> reducer, TypeSerializer<IN> serializer) {
		super(reducer);
		this.serializer = serializer;
	}

	//重写open函数，创建中间缓存结果
	@Override
	public void open() throws Exception {
		super.open();
		ValueStateDescriptor<IN> stateId = new ValueStateDescriptor<>(STATE_NAME, serializer);//key对应一个value值
		values = getPartitionedState(stateId);
	}

	//相当于在流上套了一层map操作,只是map的计算方式是 聚合上一步的结果,每次发送累计结果到下游输出
	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		IN value = element.getValue();//当前元素
		IN currentValue = values.value();//聚合值

		if (currentValue != null) {
			IN reduced = userFunction.reduce(currentValue, value);//新值
			values.update(reduced);
			output.collect(element.replace(reduced));//发送新值
		} else {
			values.update(value);
			output.collect(element.replace(value));
		}
	}
}
