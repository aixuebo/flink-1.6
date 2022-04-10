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

import org.apache.flink.annotation.Public;

import java.io.Serializable;

/**
 * Accumulators collect distributed statistics or aggregates in a from user functions
 * and operators. Each parallel instance creates and updates its own accumulator object,
 * and the different parallel instances of the accumulator are later merged.
 * merged by the system at the end of the job. The result can be obtained from the
 * result of a job execution, or from the web runtime monitor.
 *
 * The accumulators are inspired by the Hadoop/MapReduce counters.
 * 
 * The type added to the accumulator might differ from the type returned. This
 * is the case e.g. for a set-accumulator: We add single objects, but the result
 * is a set of objects.
 * 
 * @param <V>
 *            Type of values that are added to the accumulator
 * @param <R>
 *            Type of the accumulator result as it will be reported to the
 *            client
 * 做聚合---将元素v进行聚合,聚合结果是R。注意R可以序列化
 */
@Public
public interface Accumulator<V, R extends Serializable> extends Serializable, Cloneable {
	/**
	 * @param value
	 *            The value to add to the accumulator object
	 * 添加一个V,如何参与计算，生产R
	 */
	void add(V value);

	/**
	 * @return local The local value from the current UDF context
	 * 返回R
	 */
	R getLocalValue();

	/**
	 * Reset the local value. This only affects the current UDF context.
	 * 重置R
	 */
	void resetLocal();

	/**
	 * Used by system internally to merge the collected parts of an accumulator
	 * at the end of the job.
	 * 
	 * @param other Reference to accumulator to merge in.
	 * R的merge
	 */
	void merge(Accumulator<V, R> other);

	/**
	 * Duplicates the accumulator. All subclasses need to properly implement
	 * cloning and cannot throw a {@link java.lang.CloneNotSupportedException}
	 *
	 * @return The duplicated accumulator.
	 */
	Accumulator<V, R> clone();
}
