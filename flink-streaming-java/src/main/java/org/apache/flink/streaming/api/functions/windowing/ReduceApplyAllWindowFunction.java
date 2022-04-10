/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions.windowing;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.operators.translation.WrappingFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.util.Collections;

/**
 * Internal {@link AllWindowFunction} that is used for implementing a fold on a window configuration
 * that only allows {@link AllWindowFunction} and cannot directly execute a {@link ReduceFunction}.
 *
 * 不是针对key分组的窗口,计算Reduce方法
 */
@Internal
public class ReduceApplyAllWindowFunction<W extends Window, T, R>
	extends WrappingFunction<AllWindowFunction<T, R, W>>
	implements AllWindowFunction<T, R, W> {

	private static final long serialVersionUID = 1L;

	private final ReduceFunction<T> reduceFunction;

	public ReduceApplyAllWindowFunction(ReduceFunction<T> reduceFunction,
		AllWindowFunction<T, R, W> windowFunction) {
		super(windowFunction);
		this.reduceFunction = reduceFunction;
	}


	@Override
	public void apply(W window, Iterable<T> input, Collector<R> out) throws Exception {

		//先对元素集合进行预处理,使用ReduceFunction函数,将集合最终转换成一个元素
		T curr = null;
		for (T val: input) {
			if (curr == null) {
				curr = val;
			} else {
				curr = reduceFunction.reduce(curr, val);
			}
		}

		//再次调用窗口函数，虽然窗口函数要求的是全部数据源，但此时已经是只有一个元素了，因此效率会高，
		//但说实话我没get到优势.在哪层做这个聚合不都是一样的吗？难道是map阶段的预聚合？
		wrappedFunction.apply(window, Collections.singletonList(curr), out);
	}
}
