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

package org.apache.flink.streaming.runtime.streamrecord;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;

/**
 * An element in a data stream. Can be a record or a Watermark.
 * 流里面的一个元素,可以是一条记录，也可以是Watermark
 *
 * 一共四类元素 --- 自定义元素StreamRecord(由时间戳+value组成)、Watermark、StreamStatus(流的状态)、LatencyMarker
 */
@Internal
public abstract class StreamElement {

	/**
	 * Checks whether this element is a watermark.
	 * @return True, if this element is a watermark, false otherwise.
	 * 是否是Watermark元素
	 */
	public final boolean isWatermark() {
		return getClass() == Watermark.class;
	}

	/**
	 * Checks whether this element is a stream status.
	 * @return True, if this element is a stream status, false otherwise.
	 * 是否是StreamStatus元素
	 */
	public final boolean isStreamStatus() {
		return getClass() == StreamStatus.class;
	}

	/**
	 * Checks whether this element is a record.
	 * @return True, if this element is a record, false otherwise.
	 * 是否是用户定义的待处理元素
	 */
	public final boolean isRecord() {
		return getClass() == StreamRecord.class;
	}

	/**
	 * Checks whether this element is a record.
	 * @return True, if this element is a record, false otherwise.
	 * 是否是用户定义的迟到的元素
	 */
	public final boolean isLatencyMarker() {
		return getClass() == LatencyMarker.class;
	}

	/**
	 * Casts this element into a StreamRecord.
	 * @return This element as a stream record.
	 * @throws java.lang.ClassCastException Thrown, if this element is actually not a stream record.
	 * 转化成用户自定义的元素类型
	 */
	@SuppressWarnings("unchecked")
	public final <E> StreamRecord<E> asRecord() {
		return (StreamRecord<E>) this;
	}

	/**
	 * Casts this element into a Watermark.
	 * @return This element as a Watermark.
	 * @throws java.lang.ClassCastException Thrown, if this element is actually not a Watermark.
	 */
	public final Watermark asWatermark() {
		return (Watermark) this;
	}

	/**
	 * Casts this element into a StreamStatus.
	 * @return This element as a StreamStatus.
	 * @throws java.lang.ClassCastException Thrown, if this element is actually not a Stream Status.
	 */
	public final StreamStatus asStreamStatus() {
		return (StreamStatus) this;
	}

	/**
	 * Casts this element into a LatencyMarker.
	 * @return This element as a LatencyMarker.
	 * @throws java.lang.ClassCastException Thrown, if this element is actually not a LatencyMarker.
	 */
	public final LatencyMarker asLatencyMarker() {
		return (LatencyMarker) this;
	}
}
