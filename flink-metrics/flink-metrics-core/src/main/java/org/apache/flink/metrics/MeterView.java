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

package org.apache.flink.metrics;

/**
 * A MeterView provides an average rate of events per second over a given time period.
 *
 * <p>The primary advantage of this class is that the rate is neither updated by the computing thread nor for every event.
 * Instead, a history of counts is maintained that is updated in regular intervals by a background thread. From this
 * history a rate is derived on demand, which represents the average rate of events over the given time span.
 *
 * <p>Setting the time span to a low value reduces memory-consumption and will more accurately report short-term changes.
 * The minimum value possible is {@link View#UPDATE_INTERVAL_SECONDS}.
 * A high value in turn increases memory-consumption, since a longer history has to be maintained, but will result in
 * smoother transitions between rates.
 *
 * <p>The events are counted by a {@link Counter}.
 *
 * 比如统计周期是1分钟，计算该周期内,平均每秒多少条数据
 */
public class MeterView implements Meter, View {
	/** The underlying counter maintaining the count. */
	private final Counter counter;

	/** The time-span over which the average is calculated. */
	private final int timeSpanInSeconds;//统计周期

	/** Circular array containing the history of values. */
	private final long[] values;//存储数据的数组分桶

	/** The index in the array for the current time. */
	private int time = 0;//当前桶的下标

	/** The last rate we computed. */
	private double currentRate = 0;

	public MeterView(int timeSpanInSeconds) {
		this(new SimpleCounter(), timeSpanInSeconds);
	}

	public MeterView(Counter counter, int timeSpanInSeconds) {
		this.counter = counter;
		this.timeSpanInSeconds = timeSpanInSeconds - (timeSpanInSeconds % UPDATE_INTERVAL_SECONDS); //计算UPDATE_INTERVAL_SECONDS的整数倍
		this.values = new long[this.timeSpanInSeconds / UPDATE_INTERVAL_SECONDS + 1];//分桶,并且多一个桶,目的存储超出范围的数据
	}

	@Override
	public void markEvent() {
		this.counter.inc();
	}

	@Override
	public void markEvent(long n) {
		this.counter.inc(n);
	}

	@Override
	public long getCount() {
		return counter.getCount();
	}

	@Override
	public double getRate() {
		return currentRate;
	}

	//核心问题是谁周期的调用该方法
	@Override
	public void update() {
		time = (time + 1) % values.length;//切换桶下标
		values[time] = counter.getCount();//计算该时间点桶内的累计值
		currentRate =  ((double) (values[time] - values[(time + 1) % values.length]) / timeSpanInSeconds);
	}
}
