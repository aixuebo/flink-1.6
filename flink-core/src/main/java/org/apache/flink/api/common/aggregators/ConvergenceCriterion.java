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


package org.apache.flink.api.common.aggregators;

import java.io.Serializable;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.types.Value;

/**
 * Used to check for convergence.
 * 校验是否已经收敛
 */
@PublicEvolving
public interface ConvergenceCriterion<T extends Value> extends Serializable {

	/**
	 * Decide whether the iterative algorithm has converged
	 * 决定是否迭代算法已经完成收敛,即不需要再迭代了
	 * true表示收敛
	 * 参数iteration 表示算法迭代了多少轮。
	 * 参数value ,表示迭代iteration后,损失函数值是多少,是否可以收敛了
	 */
	boolean isConverged(int iteration, T value);
}
