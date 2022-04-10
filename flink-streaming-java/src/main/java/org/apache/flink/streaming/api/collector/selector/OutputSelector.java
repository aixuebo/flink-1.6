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

package org.apache.flink.streaming.api.collector.selector;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;

import java.io.Serializable;

/**
 * Interface for defining an OutputSelector for a {@link SplitStream} using
 * the {@link SingleOutputStreamOperator#split} call. Every output object of a
 * {@link SplitStream} will run through this operator to select outputs.
 *
 * @param <OUT>
 *            Type parameter of the split values.
 * 如何将一个流 拆分成 多个流。
 * DataStream.split方法转换成该流
 */
@PublicEvolving
public interface OutputSelector<OUT> extends Serializable {
	/**
	 * Method for selecting output names for the emitted objects when using the
	 * {@link SingleOutputStreamOperator#split} method. The values will be
	 * emitted only to output names which are contained in the returned
	 * iterable.
	 *
	 * @param value
	 *            Output object for which the output selection should be made.
	 * 将一个元素,输出到哪些流name中
	 *
	 * 将参数转换成迭代器
	 *
	 * 参数是输入的元素，输出是该元素被打的tag集合。
	 * 后期是需要被SplitStream的select(String... outputNames) 或者  selectOutput(String[] outputNames) 选择要哪些tag子流
	 */
	Iterable<String> select(OUT value);
}
