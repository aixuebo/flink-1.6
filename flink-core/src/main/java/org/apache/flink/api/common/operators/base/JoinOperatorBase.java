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

package org.apache.flink.api.common.operators.base;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.operators.BinaryOperatorInformation;
import org.apache.flink.api.common.operators.DualInputOperator;
import org.apache.flink.api.common.operators.util.UserCodeClassWrapper;
import org.apache.flink.api.common.operators.util.UserCodeObjectWrapper;
import org.apache.flink.api.common.operators.util.UserCodeWrapper;

//设置如何join,key如何分区到不同节点、数据join的分发优化
@Internal
public abstract class JoinOperatorBase<IN1, IN2, OUT, FT extends FlatJoinFunction<IN1, IN2, OUT>> extends DualInputOperator<IN1, IN2, OUT, FT> {

	/**
	 * An enumeration of hints, optionally usable to tell the system how exactly execute the join.
	 * 提示,仅仅是一个提示,不能决定作用,具体看代码实现是否参考该提示
	 */
	@Public
	public static enum JoinHint {

		/**
		 * Leave the choice how to do the join to the optimizer. If in doubt, the
		 * optimizer will choose a repartitioning join.
		 * flink优化器自己决定如何join
		 */
		OPTIMIZER_CHOOSES,

		/**
		 * Hint that the first join input is much smaller than the second. This results in
		 * broadcasting and hashing the first input, unless the optimizer infers that
		 * prior existing partitioning is available that is even cheaper to exploit.
		 * 第一个数据源很小,因此广播第一个数据源
		 */
		BROADCAST_HASH_FIRST,

		/**
		 * Hint that the second join input is much smaller than the first. This results in
		 * broadcasting and hashing the second input, unless the optimizer infers that
		 * prior existing partitioning is available that is even cheaper to exploit.
		 */
		BROADCAST_HASH_SECOND,

		/**
		 * Hint that the first join input is a bit smaller than the second. This results in
		 * repartitioning both inputs and hashing the first input, unless the optimizer infers that
		 * prior existing partitioning and orders are available that are even cheaper to exploit.
		 * 第一个数据集稍微小一些
		 */
		REPARTITION_HASH_FIRST,

		/**
		 * Hint that the second join input is a bit smaller than the first. This results in
		 * repartitioning both inputs and hashing the second input, unless the optimizer infers that
		 * prior existing partitioning and orders are available that are even cheaper to exploit.
		 */
		REPARTITION_HASH_SECOND,

		/**
		 * Hint that the join should repartitioning both inputs and use sorting and merging
		 * as the join strategy.
		 * 使用排序和合并策略对数据集进行重新分配
		 */
		REPARTITION_SORT_MERGE
	}

	private JoinHint joinHint = JoinHint.OPTIMIZER_CHOOSES;
	private Partitioner<?> partitioner;


	public JoinOperatorBase(UserCodeWrapper<FT> udf, BinaryOperatorInformation<IN1, IN2, OUT> operatorInfo, int[] keyPositions1, int[] keyPositions2, String name) {
		super(udf, operatorInfo, keyPositions1, keyPositions2, name);
	}

	public JoinOperatorBase(FT udf, BinaryOperatorInformation<IN1, IN2, OUT> operatorInfo, int[] keyPositions1, int[] keyPositions2, String name) {
		super(new UserCodeObjectWrapper<>(udf), operatorInfo, keyPositions1, keyPositions2, name);
	}

	public JoinOperatorBase(Class<? extends FT> udf, BinaryOperatorInformation<IN1, IN2, OUT> operatorInfo, int[] keyPositions1, int[] keyPositions2, String name) {
		super(new UserCodeClassWrapper<>(udf), operatorInfo, keyPositions1, keyPositions2, name);
	}


	public void setJoinHint(JoinHint joinHint) {
		if (joinHint == null) {
			throw new IllegalArgumentException("Join Hint must not be null.");
		}
		this.joinHint = joinHint;
	}

	public JoinHint getJoinHint() {
		return joinHint;
	}

	public void setCustomPartitioner(Partitioner<?> partitioner) {
		this.partitioner = partitioner;
	}

	public Partitioner<?> getCustomPartitioner() {
		return partitioner;
	}

}
