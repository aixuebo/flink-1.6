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

package org.apache.flink.api.java.summarize.aggregation;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.summarize.NumericColumnSummary;

import static org.apache.flink.api.java.summarize.aggregation.CompensatedSum.ZERO;

/**
 * Generic aggregator for all numeric types creates a summary of a column of numbers.
 *
 * <p>Uses the Kahan summation algorithm to avoid numeric instability when computing variance.
 * The algorithm is described in: "Scalable and Numerically Stable Descriptive Statistics in SystemML",
 * Tian et al, International Conference on Data Engineering 2012
 *
 * <p>Implementation that couldn't be generic for all numbers was pushed to subclasses.
 * For example, there isn't a generic way to calculate min, max, sum, isNan, isInfinite
 * for all numeric types so subclasses must implement these.
 *
 * @param <T> numeric type to aggregrate and create a summary, e.g. Integer, DoubleValue
 *
 * 要求参数T必须是Number类型
 * 统计最大值、最小值、sum、均值、方差数据
 */
@Internal
public abstract class NumericSummaryAggregator<T extends Number> implements Aggregator<T, NumericColumnSummary<T>> {

	private static final long serialVersionUID = 1L;

	private long nonMissingCount = 0L; // count of elements that are NOT null, NaN, or Infinite,非空,正常元素的数量
	private long nullCount = 0L;//null的数量
	private long nanCount = 0L;//NaN的数量
	private long infinityCount = 0L;//infinity的数量

	// these fields are initialized by type specific subclasses
	private Aggregator<T, T> min = initMin();
	private Aggregator<T, T> max = initMax();
	private Aggregator<T, T> sum = initSum();

	//均值
	private CompensatedSum mean = ZERO;
	/**
	 * Sum of squares of differences from the current mean (used to calculate variance).
	 *
	 * <p>The algorithm is described in: "Scalable and Numerically Stable Descriptive Statistics in SystemML",
	 * Tian et al, International Conference on Data Engineering 2012
	 * 方差
	 */
	private CompensatedSum m2 = ZERO;

	/**
	 * Add a value to the current aggregation.
	 */
	@Override
	public void aggregate(T value) {

		if (value == null) {
			nullCount++;
		}
		else if (isNan(value)) {
			nanCount++;
		}
		else if (isInfinite(value)) {
			infinityCount++;
		}
		else {
			nonMissingCount++;

			min.aggregate(value);
			max.aggregate(value);
			sum.aggregate(value);

			double doubleValue = value.doubleValue();
			double delta = doubleValue - mean.value();
			mean = mean.add(delta / nonMissingCount);
			m2 = m2.add(delta * (doubleValue - mean.value()));
		}
	}

	/**
	 * combine two aggregations.
	 */
	@Override
	public void combine(Aggregator<T, NumericColumnSummary<T>> otherSameType) {
		NumericSummaryAggregator<T> other = (NumericSummaryAggregator<T>) otherSameType;

		nullCount += other.nullCount;
		nanCount += other.nanCount;
		infinityCount += other.infinityCount;

		if (nonMissingCount == 0) {
			nonMissingCount = other.nonMissingCount;

			min = other.min;
			max = other.max;

			sum = other.sum;
			mean = other.mean;
			m2 = other.m2;
		}
		else if (other.nonMissingCount != 0) {
			long combinedCount = nonMissingCount + other.nonMissingCount;

			min.combine(other.min);
			max.combine(other.max);

			sum.combine(other.sum);

			double deltaMean = other.mean.value() - mean.value();
			mean = mean.add(deltaMean * other.nonMissingCount / combinedCount);
			m2 = m2.add(other.m2).add(deltaMean * deltaMean * nonMissingCount * other.nonMissingCount / combinedCount);

			nonMissingCount = combinedCount;
		}
	}

	@Override
	public NumericColumnSummary<T> result() {

		Double variance = null;
		if (nonMissingCount > 1) {
			variance = m2.value() / (nonMissingCount - 1);
		}

		return new NumericColumnSummary<T>(
			nonMissingCount,
			nullCount,
			nanCount,
			infinityCount,
			// if nonMissingCount was zero some fields should be undefined
			nonMissingCount == 0 ? null : min.result(),
			nonMissingCount == 0 ? null : max.result(),
			nonMissingCount == 0 ? null : sum.result(),
			nonMissingCount == 0 ? null : mean.value(),
			variance,
			variance == null ? null : Math.sqrt(variance) // standard deviation
		);
	}

	// there isn't a generic way to calculate min, max, sum, isNan, isInfinite for all numeric types
	// so subclasses must implement these

	protected abstract Aggregator<T, T> initMin();//最小值集合器

	protected abstract Aggregator<T, T> initMax();//最大值集合器

	protected abstract Aggregator<T, T> initSum();//求sum集合器

	protected abstract boolean isNan(T number);//是否是NaN,只有Float和Double类型才有NaN

	protected abstract boolean isInfinite(T number);//是否是Infinite,只有Float和Double类型才有Infinite

}
