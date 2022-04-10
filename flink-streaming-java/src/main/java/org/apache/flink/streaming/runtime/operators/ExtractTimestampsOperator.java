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

package org.apache.flink.streaming.runtime.operators;

import org.apache.flink.streaming.api.functions.TimestampExtractor;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;

/**
 * A {@link org.apache.flink.streaming.api.operators.StreamOperator} for extracting timestamps
 * from user elements and assigning them as the internal timestamp of the {@link StreamRecord}.
 *
 * @param <T> The type of the input elements
 *
 * @deprecated Subsumed by {@link TimestampsAndPeriodicWatermarksOperator} and
 *             {@link TimestampsAndPunctuatedWatermarksOperator}.
 *
//1.设置定时任务，从函数中提取水印时间戳,发送给下游。
//2.每次从数据元素中提取事件时间戳，更新元素时间戳信息；每次从数据元素中提取水印时间戳，如果水印时间戳发生变化,则随时发送给下游
 */
@Deprecated
public class ExtractTimestampsOperator<T>
		extends AbstractUdfStreamOperator<T, TimestampExtractor<T>>
		implements OneInputStreamOperator<T, T>, ProcessingTimeCallback {

	private static final long serialVersionUID = 1L;

	private transient long watermarkInterval;

	private transient long currentWatermark;

	public ExtractTimestampsOperator(TimestampExtractor<T> extractor) {
		super(extractor);
		chainingStrategy = ChainingStrategy.ALWAYS;
	}

	@Override
	public void open() throws Exception {
		super.open();
		//水印时间间隔,设置定时任务,打印水印
		watermarkInterval = getExecutionConfig().getAutoWatermarkInterval();
		if (watermarkInterval > 0) {
			long now = getProcessingTimeService().getCurrentProcessingTime();
			getProcessingTimeService().registerTimer(now + watermarkInterval, this);
		}
		currentWatermark = Long.MIN_VALUE;
	}

	@Override
	public void processElement(StreamRecord<T> element) throws Exception {
		//提起新的时间戳,覆盖原始数据,发送给下游
		long newTimestamp = userFunction.extractTimestamp(element.getValue(), element.getTimestamp());
		output.collect(element.replace(element.getValue(), newTimestamp));

		//提取水印时间戳,如果水印时间戳比历史大,则发送水印时间戳
		long watermark = userFunction.extractWatermark(element.getValue(), newTimestamp);
		if (watermark > currentWatermark) {
			currentWatermark = watermark;
			output.emitWatermark(new Watermark(currentWatermark));
		}
	}

	//定期发送水印时间戳
	@Override
	public void onProcessingTime(long timestamp) throws Exception {
		// register next timer
		long newWatermark = userFunction.getCurrentWatermark();
		if (newWatermark > currentWatermark) {
			currentWatermark = newWatermark;
			// emit watermark
			output.emitWatermark(new Watermark(currentWatermark));
		}

		long now = getProcessingTimeService().getCurrentProcessingTime();
		getProcessingTimeService().registerTimer(now + watermarkInterval, this);
	}

	//忽略,感觉不会发生这种情况
	@Override
	public void processWatermark(Watermark mark) throws Exception {
		// if we receive a Long.MAX_VALUE watermark we forward it since it is used
		// to signal the end of input and to not block watermark progress downstream
		if (mark.getTimestamp() == Long.MAX_VALUE && mark.getTimestamp() > currentWatermark) {
			currentWatermark = Long.MAX_VALUE;
			output.emitWatermark(mark);
		}
	}
}
