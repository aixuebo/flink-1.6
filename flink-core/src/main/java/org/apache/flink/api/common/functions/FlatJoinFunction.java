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

package org.apache.flink.api.common.functions;

import org.apache.flink.annotation.Public;
import org.apache.flink.util.Collector;

import java.io.Serializable;

/**
 * Interface for Join functions. Joins combine two data sets by joining their
 * elements on specified keys. This function is called with each pair of joining elements.
 *
 * <p>This particular variant of the join function supports to return zero, one, or more result values
 * per pair of joining values.
 *
 * <p>By default, the joins follows strictly the semantics of an "inner join" in SQL.
 * the semantics are those of an "inner join", meaning that elements are filtered out
 * if their key is not contained in the other data set.
 *
 * <p>The basic syntax for using Join on two data sets is as follows:
 * <pre>{@code
 * DataSet<X> set1 = ...;
 * DataSet<Y> set2 = ...;
 *
 * set1.join(set2).where(<key-definition>).equalTo(<key-definition>).with(new MyJoinFunction());
 * }</pre>
 *
 * <p>{@code set1} is here considered the first input, {@code set2} the second input.
 *
 * <p>The Join function is an optional part of a join operation. If no JoinFunction is provided,
 * the result of the operation is a sequence of 2-tuples, where the elements in the tuple are those that
 * the JoinFunction would have been invoked with.
 *
 * <p>Note: You can use a {@link CoGroupFunction} to perform an outer join.
 *
 * @param <IN1> The type of the elements in the first input.
 * @param <IN2> The type of the elements in the second input.
 * @param <OUT> The type of the result elements.
 * FlatJoinFunction,(I1,I2) --> List<O>的转换
 */
@Public
@FunctionalInterface
public interface FlatJoinFunction<IN1, IN2, OUT> extends Function, Serializable {

	/**
	 * The join method, called once per joined pair of elements.
	 *
	 * @param first The element from first input.
	 * @param second The element from second input.
	 * @param out The collector used to return zero, one, or more elements.
	 *
	 * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation
	 *                   to fail and may trigger recovery.
	 * 能进入该方法时,已经确保first和second的数据已经被on条件选中,即可以直接对两条数据做处理了
	 *
	 * 注意 由于涉及到left join等情况，因此fitst或者second存在可能是null的情况
	 */
	void join (IN1 first, IN2 second, Collector<OUT> out) throws Exception;
}
