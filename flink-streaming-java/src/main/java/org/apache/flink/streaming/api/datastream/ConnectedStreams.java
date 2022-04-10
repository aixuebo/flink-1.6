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

package org.apache.flink.streaming.api.datastream;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.operators.co.CoProcessOperator;
import org.apache.flink.streaming.api.operators.co.CoStreamFlatMap;
import org.apache.flink.streaming.api.operators.co.CoStreamMap;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;

import static java.util.Objects.requireNonNull;

/**
 * ConnectedStreams represent two connected streams of (possibly) different data types.
 * Connected streams are useful for cases where operations on one stream directly
 * affect the operations on the other stream, usually via shared state between the streams.
 *
 * <p>An example for the use of connected streams would be to apply rules that change over time
 * onto another stream. One of the connected streams has the rules, the other stream the
 * elements to apply the rules to. The operation on the connected stream maintains the
 * current set of rules in the state. It may receive either a rule update and update the state
 * or a data element and apply the rules in the state to the element.
 *
 * <p>The connected stream can be conceptually viewed as a union stream of an Either type, that
 * holds either the first stream's type or the second stream's type.
 *
 * @param <IN1> Type of the first input data steam.
 * @param <IN2> Type of the second input data stream.
 *
 * 两个不同数据结构类型的DataStream做连接,输出相同类型的结果
 *
经过graph的计算后,会知道two来自于两个输入的输出结果，因此假设two有10个分区，
则依赖的两个任务最重也会把内容打到这10个分区内。


核心:本身不做join。只是把两个数据源的数据接入，分别处理不同数据源数据，统一按照key分发到下游。只是两个数据源共同同一组下游节点size。
下游可以随心所欲再近些keyBy等聚合操作，本步骤只是把两个数据源对接成一个输出的过程。
 *
 */
@Public
public class ConnectedStreams<IN1, IN2> {

	protected final StreamExecutionEnvironment environment;
	protected final DataStream<IN1> inputStream1;
	protected final DataStream<IN2> inputStream2;

	protected ConnectedStreams(StreamExecutionEnvironment env, DataStream<IN1> input1, DataStream<IN2> input2) {
		this.environment = requireNonNull(env);
		this.inputStream1 = requireNonNull(input1);
		this.inputStream2 = requireNonNull(input2);
	}

	public StreamExecutionEnvironment getExecutionEnvironment() {
		return environment;
	}

	/**
	 * Returns the first {@link DataStream}.
	 *
	 * @return The first DataStream.
	 */
	public DataStream<IN1> getFirstInput() {
		return inputStream1;
	}

	/**
	 * Returns the second {@link DataStream}.
	 *
	 * @return The second DataStream.
	 */
	public DataStream<IN2> getSecondInput() {
		return inputStream2;
	}

	/**
	 * Gets the type of the first input.
	 *
	 * @return The type of the first input
	 */
	public TypeInformation<IN1> getType1() {
		return inputStream1.getType();
	}

	/**
	 * Gets the type of the second input.
	 *
	 * @return The type of the second input
	 */
	public TypeInformation<IN2> getType2() {
		return inputStream2.getType();
	}

	/**
	 * KeyBy operation for connected data stream. Assigns keys to the elements of
	 * input1 and input2 according to keyPosition1 and keyPosition2.
	 *
	 * @param keyPosition1
	 *            The field used to compute the hashcode of the elements in the
	 *            first input stream.
	 * @param keyPosition2
	 *            The field used to compute the hashcode of the elements in the
	 *            second input stream.
	 * @return The grouped {@link ConnectedStreams}
	 */
	public ConnectedStreams<IN1, IN2> keyBy(int keyPosition1, int keyPosition2) {
		return new ConnectedStreams<>(this.environment, inputStream1.keyBy(keyPosition1),//KeyedStream<T, KEY> extends DataStream<T>
				inputStream2.keyBy(keyPosition2));
	}

	/**
	 * KeyBy operation for connected data stream. Assigns keys to the elements of
	 * input1 and input2 according to keyPositions1 and keyPositions2.
	 *
	 * @param keyPositions1
	 *            The fields used to group the first input stream.
	 * @param keyPositions2
	 *            The fields used to group the second input stream.
	 * @return The grouped {@link ConnectedStreams}
	 */
	public ConnectedStreams<IN1, IN2> keyBy(int[] keyPositions1, int[] keyPositions2) {
		return new ConnectedStreams<>(environment, inputStream1.keyBy(keyPositions1),
				inputStream2.keyBy(keyPositions2));
	}

	/**
	 * KeyBy operation for connected data stream using key expressions. Assigns keys to
	 * the elements of input1 and input2 according to field1 and field2. A field
	 * expression is either the name of a public field or a getter method with
	 * parentheses of the {@link DataStream}S underlying type. A dot can be used
	 * to drill down into objects, as in {@code "field1.getInnerField2()" }.
	 *
	 * @param field1
	 *            The grouping expression for the first input
	 * @param field2
	 *            The grouping expression for the second input
	 * @return The grouped {@link ConnectedStreams}
	 */
	public ConnectedStreams<IN1, IN2> keyBy(String field1, String field2) {
		return new ConnectedStreams<>(environment, inputStream1.keyBy(field1),
				inputStream2.keyBy(field2));
	}

	/**
	 * KeyBy operation for connected data stream using key expressions.
	 * the elements of input1 and input2 according to fields1 and fields2. A
	 * field expression is either the name of a public field or a getter method
	 * with parentheses of the {@link DataStream}S underlying type. A dot can be
	 * used to drill down into objects, as in {@code "field1.getInnerField2()" }
	 * .
	 *
	 * @param fields1
	 *            The grouping expressions for the first input
	 * @param fields2
	 *            The grouping expressions for the second input
	 * @return The grouped {@link ConnectedStreams}
	 */
	public ConnectedStreams<IN1, IN2> keyBy(String[] fields1, String[] fields2) {
		return new ConnectedStreams<>(environment, inputStream1.keyBy(fields1),
				inputStream2.keyBy(fields2));
	}

	/**
	 * KeyBy operation for connected data stream. Assigns keys to the elements of
	 * input1 and input2 using keySelector1 and keySelector2.
	 *
	 * @param keySelector1
	 *            The {@link KeySelector} used for grouping the first input
	 * @param keySelector2
	 *            The {@link KeySelector} used for grouping the second input
	 * @return The partitioned {@link ConnectedStreams}
	 */
	public ConnectedStreams<IN1, IN2> keyBy(KeySelector<IN1, ?> keySelector1, KeySelector<IN2, ?> keySelector2) {
		return new ConnectedStreams<>(environment, inputStream1.keyBy(keySelector1),
				inputStream2.keyBy(keySelector2));
	}

	/**
	 * KeyBy operation for connected data stream. Assigns keys to the elements of
	 * input1 and input2 using keySelector1 and keySelector2 with explicit type information
	 * for the common key type.
	 *
	 * @param keySelector1
	 *            The {@link KeySelector} used for grouping the first input
	 * @param keySelector2
	 *            The {@link KeySelector} used for grouping the second input
	 * @param keyType The type information of the common key type.
	 * @return The partitioned {@link ConnectedStreams}
	 */
	public <KEY> ConnectedStreams<IN1, IN2> keyBy(
			KeySelector<IN1, KEY> keySelector1,
			KeySelector<IN2, KEY> keySelector2,
			TypeInformation<KEY> keyType) {
		return new ConnectedStreams<>(
			environment,
			inputStream1.keyBy(keySelector1, keyType),
			inputStream2.keyBy(keySelector2, keyType));
	}

	/**
	 * Applies a CoMap transformation on a {@link ConnectedStreams} and maps
	 * the output to a common type. The transformation calls a
	 * {@link CoMapFunction#map1} for each element of the first input and
	 * {@link CoMapFunction#map2} for each element of the second input. Each
	 * CoMapFunction call returns exactly one element.
	 *
	 * @param coMapper The CoMapFunction used to jointly transform the two input DataStreams
	 * @return The transformed {@link DataStream}
	 */
	public <R> SingleOutputStreamOperator<R> map(CoMapFunction<IN1, IN2, R> coMapper) {

		//计算输出类型
		TypeInformation<R> outTypeInfo = TypeExtractor.getBinaryOperatorReturnType(
			coMapper,
			CoMapFunction.class,
			0,
			1,
			2,
			TypeExtractor.NO_INDEX,
			getType1(),
			getType2(),
			Utils.getCallLocationName(),
			true);

		return transform("Co-Map", outTypeInfo, new CoStreamMap<>(inputStream1.clean(coMapper)));

	}

	/**
	 * Applies a CoFlatMap transformation on a {@link ConnectedStreams} and
	 * maps the output to a common type. The transformation calls a
	 * {@link CoFlatMapFunction#flatMap1} for each element of the first input
	 * and {@link CoFlatMapFunction#flatMap2} for each element of the second
	 * input. Each CoFlatMapFunction call returns any number of elements
	 * including none.
	 *
	 * @param coFlatMapper
	 *            The CoFlatMapFunction used to jointly transform the two input
	 *            DataStreams
	 * @return The transformed {@link DataStream}
	 */
	public <R> SingleOutputStreamOperator<R> flatMap(
			CoFlatMapFunction<IN1, IN2, R> coFlatMapper) {

		//计算输出类型
		TypeInformation<R> outTypeInfo = TypeExtractor.getBinaryOperatorReturnType(
			coFlatMapper,
			CoFlatMapFunction.class,
			0,
			1,
			2,
			TypeExtractor.NO_INDEX,
			getType1(),
			getType2(),
			Utils.getCallLocationName(),
			true);

		//注意:inputStream1.clean(coFlatMapper) 只是在让coFlatMapper函数序列化,与inputStream1无关。使用inputStream1或者inputStream2都可以
		return transform("Co-Flat Map", outTypeInfo, new CoStreamFlatMap<>(inputStream1.clean(coFlatMapper)));
	}

	/**
	 * Applies the given {@link CoProcessFunction} on the connected input streams,
	 * thereby creating a transformed output stream.
	 *
	 * <p>The function will be called for every element in the input streams and can produce zero or
	 * more output elements. Contrary to the {@link #flatMap(CoFlatMapFunction)} function, this
	 * function can also query the time and set timers. When reacting to the firing of set timers
	 * the function can directly emit elements and/or register yet more timers.
	 *
	 * @param coProcessFunction The {@link CoProcessFunction} that is called for each element
	 *                      in the stream.
	 *
	 * @param <R> The type of elements emitted by the {@code CoProcessFunction}.
	 *
	 * @return The transformed {@link DataStream}.
	 */
	@PublicEvolving
	public <R> SingleOutputStreamOperator<R> process(
			CoProcessFunction<IN1, IN2, R> coProcessFunction) {

		TypeInformation<R> outTypeInfo = TypeExtractor.getBinaryOperatorReturnType(
			coProcessFunction,
			CoProcessFunction.class,
			0,
			1,
			2,
			TypeExtractor.NO_INDEX,
			getType1(),
			getType2(),
			Utils.getCallLocationName(),
			true);

		return process(coProcessFunction, outTypeInfo);
	}

	/**
	 * Applies the given {@link CoProcessFunction} on the connected input streams,
	 * thereby creating a transformed output stream.
	 *
	 * <p>The function will be called for every element in the input streams and can produce zero
	 * or more output elements. Contrary to the {@link #flatMap(CoFlatMapFunction)} function,
	 * this function can also query the time and set timers. When reacting to the firing of set
	 * timers the function can directly emit elements and/or register yet more timers.
	 *
	 * @param coProcessFunction The {@link CoProcessFunction} that is called for each element
	 *                      in the stream.
	 *
	 * @param <R> The type of elements emitted by the {@code CoProcessFunction}.
	 *
	 * @return The transformed {@link DataStream}.
	 */
	@Internal
	public <R> SingleOutputStreamOperator<R> process(
			CoProcessFunction<IN1, IN2, R> coProcessFunction,
			TypeInformation<R> outputType) {

		TwoInputStreamOperator<IN1, IN2, R> operator;

		if ((inputStream1 instanceof KeyedStream) && (inputStream2 instanceof KeyedStream)) {
			operator = new KeyedCoProcessOperator<>(inputStream1.clean(coProcessFunction));
		} else {
			operator = new CoProcessOperator<>(inputStream1.clean(coProcessFunction));
		}

		return transform("Co-Process", outputType, operator);
	}

	@PublicEvolving
	public <R> SingleOutputStreamOperator<R> transform(String functionName,//函数,比如co-map
			TypeInformation<R> outTypeInfo,//返回值
			TwoInputStreamOperator<IN1, IN2, R> operator) {

		// read the output type of the input Transforms to coax out errors about MissingTypeInfo
		inputStream1.getType();
		inputStream2.getType();

		TwoInputTransformation<IN1, IN2, R> transform = new TwoInputTransformation<>(
				inputStream1.getTransformation(),
				inputStream2.getTransformation(),
				functionName,
				operator,
				outTypeInfo,
				environment.getParallelism());

		if (inputStream1 instanceof KeyedStream && inputStream2 instanceof KeyedStream) {//说明两个输入已经是做了keyby操作
			KeyedStream<IN1, ?> keyedInput1 = (KeyedStream<IN1, ?>) inputStream1;
			KeyedStream<IN2, ?> keyedInput2 = (KeyedStream<IN2, ?>) inputStream2;

			//必须确保两个流的key类型是相同的
			TypeInformation<?> keyType1 = keyedInput1.getKeyType();
			TypeInformation<?> keyType2 = keyedInput2.getKeyType();
			if (!(keyType1.canEqual(keyType2) && keyType1.equals(keyType2))) {
				throw new UnsupportedOperationException("Key types if input KeyedStreams " +
						"don't match: " + keyType1 + " and " + keyType2 + ".");
			}

			//设置 用于shuffle,每一个流的元素如何转换成key,以及key的类型
			transform.setStateKeySelectors(keyedInput1.getKeySelector(), keyedInput2.getKeySelector());
			transform.setStateKeyType(keyType1);
		}

		@SuppressWarnings({ "unchecked", "rawtypes" })
		SingleOutputStreamOperator<R> returnStream = new SingleOutputStreamOperator(environment, transform);

		getExecutionEnvironment().addOperator(transform);

		return returnStream;
	}
}
