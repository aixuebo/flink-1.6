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
package org.apache.flink.runtime.operators.util.metrics;

import org.apache.flink.metrics.Counter;
import org.apache.flink.util.Collector;

/**
 * 对Collector进行包装，包装一层计数器
 * @param <OUT>
 */
public class CountingCollector<OUT> implements Collector<OUT> {
	private final Collector<OUT> collector;//用于输出数据流
	private final Counter numRecordsOut;

	public CountingCollector(Collector<OUT> collector, Counter numRecordsOut) {
		this.collector = collector;
		this.numRecordsOut = numRecordsOut;
	}

	@Override
	public void collect(OUT record) {
		this.numRecordsOut.inc();//计数器
		this.collector.collect(record);//输出
	}

	@Override
	public void close() {
		this.collector.close();
	}
}
