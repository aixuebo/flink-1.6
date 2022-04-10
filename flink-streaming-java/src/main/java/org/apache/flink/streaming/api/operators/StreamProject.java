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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * A {@link StreamOperator} for executing projections on streams.
 * 投影操作
 */
@Internal
public class StreamProject<IN, OUT extends Tuple>
		extends AbstractStreamOperator<OUT>
		implements OneInputStreamOperator<IN, OUT> {

	private static final long serialVersionUID = 1L;

	private TypeSerializer<OUT> outSerializer;
	private int[] fields;//提取的每一个元素的位置
	private int numFields;//提取多少个元素

	private transient OUT outTuple;

	public StreamProject(int[] fields, TypeSerializer<OUT> outSerializer) {
		this.fields = fields;
		this.numFields = this.fields.length;
		this.outSerializer = outSerializer;

		chainingStrategy = ChainingStrategy.ALWAYS;
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		for (int i = 0; i < this.numFields; i++) {//提取多少个元素
			outTuple.setField(((Tuple) element.getValue()).getField(fields[i]), i);//设置第i和元素,对应的值
		}
		output.collect(element.replace(outTuple));//代替原有的tuple,保留原有时间戳信息
	}

	@Override
	public void open() throws Exception {
		super.open();
		outTuple = outSerializer.createInstance();//生产一个输出tuple对象
	}
}
