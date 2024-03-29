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

package org.apache.flink.api.common.accumulators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.OptionalFailure;
import org.apache.flink.util.SerializedValue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Helper functions for the interaction with {@link Accumulator}.
 * 聚合函数的工具类
 */
@Internal
public class AccumulatorHelper {
	private static final Logger LOG = LoggerFactory.getLogger(AccumulatorHelper.class);

	/**
	 * Merge two collections of accumulators. The second will be merged into the
	 * first.
	 *
	 * @param target
	 *            The collection of accumulators that will be updated
	 * @param toMerge
	 *            The collection of accumulators that will be merged into the
	 *            other
	 */
	public static void mergeInto(Map<String, OptionalFailure<Accumulator<?, ?>>> target, Map<String, Accumulator<?, ?>> toMerge) {
		for (Map.Entry<String, Accumulator<?, ?>> otherEntry : toMerge.entrySet()) {
			OptionalFailure<Accumulator<?, ?>> ownAccumulator = target.get(otherEntry.getKey());
			if (ownAccumulator == null) {
				// Create initial counter (copy!)
				target.put(
					otherEntry.getKey(),
					wrapUnchecked(otherEntry.getKey(), () -> otherEntry.getValue().clone()));
			}
			else if (ownAccumulator.isFailure()) {
				continue;
			}
			else {
				Accumulator<?, ?> accumulator = ownAccumulator.getUnchecked();
				// Both should have the same type
				compareAccumulatorTypes(otherEntry.getKey(),
					accumulator.getClass(), otherEntry.getValue().getClass());
				// Merge target counter with other counter

				target.put(
					otherEntry.getKey(),
					wrapUnchecked(otherEntry.getKey(), () -> mergeSingle(accumulator, otherEntry.getValue().clone())));
			}
		}
	}

	/**
	 * Workaround method for type safety.
	 * target.merge(toMerge),即将toMerge的数据复制到target中
	 */
	private static <V, R extends Serializable> Accumulator<V, R> mergeSingle(Accumulator<?, ?> target,
																			 Accumulator<?, ?> toMerge) {
		@SuppressWarnings("unchecked")
		Accumulator<V, R> typedTarget = (Accumulator<V, R>) target;

		@SuppressWarnings("unchecked")
		Accumulator<V, R> typedToMerge = (Accumulator<V, R>) toMerge;

		typedTarget.merge(typedToMerge);

		return typedTarget;
	}

	/**
	 * Compare both classes and throw {@link UnsupportedOperationException} if
	 * they differ.
	 * 属于校验方式,确保两个class是同一个类出来的
	 */
	@SuppressWarnings("rawtypes")
	public static void compareAccumulatorTypes(
			Object name,
			Class<? extends Accumulator> first,
			Class<? extends Accumulator> second) throws UnsupportedOperationException {
		if (first == null || second == null) {
			throw new NullPointerException();
		}

		if (first != second) {
			if (!first.getName().equals(second.getName())) {//class都不相同,抛异常
				throw new UnsupportedOperationException("The accumulator object '" + name
					+ "' was created with two different types: " + first.getName() + " and " + second.getName());
			} else {
				// damn, name is the same, but different classloaders, class的名字虽然相同,但不是同一个classloaders --- 不清楚怎么判断不是同一个classloaders的
				throw new UnsupportedOperationException("The accumulator object '" + name
						+ "' was created with two different classes: " + first + " and " + second
						+ " Both have the same type (" + first.getName() + ") but different classloaders: "
						+ first.getClassLoader() + " and " + second.getClassLoader());
			}
		}
	}

	/**
	 * Transform the Map with accumulators into a Map containing only the
	 * results.
	 * 对多个聚合器的结果进行统一输出。
	 * 输出的Map,key是聚合器的名字,value是聚合器的结果值
	 */
	public static Map<String, OptionalFailure<Object>> toResultMap(Map<String, Accumulator<?, ?>> accumulators) {
		Map<String, OptionalFailure<Object>> resultMap = new HashMap<>();
		for (Map.Entry<String, Accumulator<?, ?>> entry : accumulators.entrySet()) {
			resultMap.put(entry.getKey(), wrapUnchecked(entry.getKey(), () -> entry.getValue().getLocalValue()));
		}
		return resultMap;
	}

	//包装器 -- 要么返回值,要么返回异常
	private static <R> OptionalFailure<R> wrapUnchecked(String name, Supplier<R> supplier) {
		return OptionalFailure.createFrom(() -> {
			try {
				return supplier.get();
			} catch (RuntimeException ex) {
				LOG.error("Unexpected error while handling accumulator [" + name + "]", ex);
				throw new FlinkException(ex);
			}
		});
	}

	//打印输出map的数据情况 --- 即数据转换成字符串形式
	public static String getResultsFormatted(Map<String, Object> map) {
		StringBuilder builder = new StringBuilder();
		for (Map.Entry<String, Object> entry : map.entrySet()) {
			builder
				.append("- ")
				.append(entry.getKey())
				.append(" (")
				.append(entry.getValue().getClass().getName())
				.append(")");
			if (entry.getValue() instanceof Collection) {
				builder.append(" [").append(((Collection) entry.getValue()).size()).append(" elements]");//如果是集合,只需要打印集合包含了多少个元素即可
			} else {
				builder.append(": ").append(entry.getValue().toString());
			}
			builder.append(System.lineSeparator());
		}
		return builder.toString();
	}

	//聚合数据复制
	public static Map<String, Accumulator<?, ?>> copy(Map<String, Accumulator<?, ?>> accumulators) {
		Map<String, Accumulator<?, ?>> result = new HashMap<String, Accumulator<?, ?>>();

		for (Map.Entry<String, Accumulator<?, ?>> entry: accumulators.entrySet()){
			result.put(entry.getKey(), entry.getValue().clone());
		}

		return result;
	}

	/**
	 * Takes the serialized accumulator results and tries to deserialize them using the provided
	 * class loader.
	 * @param serializedAccumulators The serialized accumulator results.
	 * @param loader The class loader to use.
	 * @return The deserialized accumulator results.
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public static Map<String, OptionalFailure<Object>> deserializeAccumulators(
			Map<String, SerializedValue<OptionalFailure<Object>>> serializedAccumulators,
			ClassLoader loader) throws IOException, ClassNotFoundException {

		if (serializedAccumulators == null || serializedAccumulators.isEmpty()) {
			return Collections.emptyMap();
		}

		Map<String, OptionalFailure<Object>> accumulators = new HashMap<>(serializedAccumulators.size());

		for (Map.Entry<String, SerializedValue<OptionalFailure<Object>>> entry : serializedAccumulators.entrySet()) {

			OptionalFailure<Object> value = null;
			if (entry.getValue() != null) {
				value = entry.getValue().deserializeValue(loader);
			}

			accumulators.put(entry.getKey(), value);
		}

		return accumulators;
	}
}
