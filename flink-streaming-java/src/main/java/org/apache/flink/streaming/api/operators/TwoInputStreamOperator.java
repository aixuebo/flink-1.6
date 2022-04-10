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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * Interface for stream operators with two inputs. Use
 * {@link org.apache.flink.streaming.api.operators.AbstractStreamOperator} as a base class if
 * you want to implement a custom operator.
 *
 * @param <IN1> The input type of the operator
 * @param <IN2> The input type of the operator
 * @param <OUT> The output type of the operator
 *
 * stream1.coGroup(stream2)
 * 简单理解就是当stream1数据到来时，会调用flatMap1方法，stream2收到数据之时，会调用flatMap2方法
 *
 * Join、CoGroup和CoFlatMap这三个运算符都能够将双数据流转换为单个数据流。
 * Join和CoGroup会根据指定的条件进行数据配对操作，不同的是Join只输出匹配成功的数据对，
 * CoGroup无论是否有匹配都会输出。
 * CoFlatMap没有匹配操作，只是分别去接收两个流的输入。
 * 大家可以根据具体的业务需求，选择不同的双流操作。
 */
@PublicEvolving
public interface TwoInputStreamOperator<IN1, IN2, OUT> extends StreamOperator<OUT> {

	/**
	 * Processes one element that arrived on the first input of this two-input operator.
	 * This method is guaranteed to not be called concurrently with other methods of the operator.
	 */
	void processElement1(StreamRecord<IN1> element) throws Exception;

	/**
	 * Processes one element that arrived on the second input of this two-input operator.
	 * This method is guaranteed to not be called concurrently with other methods of the operator.
	 */
	void processElement2(StreamRecord<IN2> element) throws Exception;

	/**
	 * Processes a {@link Watermark} that arrived on the first input of this two-input operator.
	 * This method is guaranteed to not be called concurrently with other methods of the operator.
	 *
	 * @see org.apache.flink.streaming.api.watermark.Watermark
	 */
	void processWatermark1(Watermark mark) throws Exception;

	/**
	 * Processes a {@link Watermark} that arrived on the second input of this two-input operator.
	 * This method is guaranteed to not be called concurrently with other methods of the operator.
	 *
	 * @see org.apache.flink.streaming.api.watermark.Watermark
	 */
	void processWatermark2(Watermark mark) throws Exception;

	/**
	 * Processes a {@link LatencyMarker} that arrived on the first input of this two-input operator.
	 * This method is guaranteed to not be called concurrently with other methods of the operator.
	 *
	 * @see org.apache.flink.streaming.runtime.streamrecord.LatencyMarker
	 */
	void processLatencyMarker1(LatencyMarker latencyMarker) throws Exception;

	/**
	 * Processes a {@link LatencyMarker} that arrived on the second input of this two-input operator.
	 * This method is guaranteed to not be called concurrently with other methods of the operator.
	 *
	 * @see org.apache.flink.streaming.runtime.streamrecord.LatencyMarker
	 */
	void processLatencyMarker2(LatencyMarker latencyMarker) throws Exception;

}
