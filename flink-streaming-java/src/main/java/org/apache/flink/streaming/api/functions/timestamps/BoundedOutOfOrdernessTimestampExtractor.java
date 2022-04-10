/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions.timestamps;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * This is a {@link AssignerWithPeriodicWatermarks} used to emit Watermarks that lag behind the element with
 * the maximum timestamp (in event time) seen so far by a fixed amount of time, <code>t_late</code>. This can
 * help reduce the number of elements that are ignored due to lateness when computing the final result for a
 * given window, in the case where we know that elements arrive no later than <code>t_late</code> units of time
 * after the watermark that signals that the system event-time has advanced past their (event-time) timestamp.
 * */
public abstract class BoundedOutOfOrdernessTimestampExtractor<T> implements AssignerWithPeriodicWatermarks<T> {

	private static final long serialVersionUID = 1L;

	/** The current maximum timestamp seen so far.
	 * 记录最大的时间戳
	 **/
	private long currentMaxTimestamp;

	/** The timestamp of the last emitted watermark.
	 * 记录生一次发射Watermark的时间戳
	 **/
	private long lastEmittedWatermark = Long.MIN_VALUE;

	/**
	 * The (fixed) interval between the maximum seen timestamp seen in the records
	 * and that of the watermark to be emitted.
	 * Watermark需要将事件时间戳调慢多久。
	 */
	private final long maxOutOfOrderness;

	public BoundedOutOfOrdernessTimestampExtractor(Time maxOutOfOrderness) {
		if (maxOutOfOrderness.toMilliseconds() < 0) {
			throw new RuntimeException("Tried to set the maximum allowed " +
				"lateness to " + maxOutOfOrderness + ". This parameter cannot be negative.");
		}
		this.maxOutOfOrderness = maxOutOfOrderness.toMilliseconds();
		////防止内存溢出,做的特殊处理,因为Long.MIN_VALUE已经最小了,如果在-maxOutOfOrderness,那肯定就是非常大的值了。
		this.currentMaxTimestamp = Long.MIN_VALUE + this.maxOutOfOrderness;
	}

	public long getMaxOutOfOrdernessInMillis() {
		return maxOutOfOrderness;
	}

	/**
	 * Extracts the timestamp from the given element.
	 *
	 * @param element The element that the timestamp is extracted from.
	 * @return The new timestamp.
	 * 提取元素时间戳
	 */
	public abstract long extractTimestamp(T element);

	//生产Watermark
	@Override
	public final Watermark getCurrentWatermark() {
		// this guarantees that the watermark never goes backwards.
		long potentialWM = currentMaxTimestamp - maxOutOfOrderness; //先调慢时间
		if (potentialWM >= lastEmittedWatermark) {
			lastEmittedWatermark = potentialWM;
		}
		//发射最新的Watermark,看代码,可能多次发送重复的Watermark,因为没有新的数据进来,currentMaxTimestamp一直保持不变
		return new Watermark(lastEmittedWatermark);
	}

	@Override
	public final long extractTimestamp(T element, long previousElementTimestamp) {
		long timestamp = extractTimestamp(element);//提取时间戳
		if (timestamp > currentMaxTimestamp) {//更新最大时间戳
			currentMaxTimestamp = timestamp;
		}
		return timestamp;
	}
}
