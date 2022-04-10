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

package org.apache.flink.runtime.io.network.api.writer;

import org.apache.flink.core.io.IOReadableWritable;

/**
 * The {@link ChannelSelector} determines to which logical channels a record
 * should be written to.
 *
 * @param <T> the type of record which is sent through the attached output gate
 * 输出通道选择器
 */
public interface ChannelSelector<T extends IOReadableWritable> {

	/**
	 * Returns the logical channel indexes, to which the given record should be
	 * written.
	 *
	 * @param record      the record to the determine the output channels for 原始记录
	 * @param numChannels the total number of output channels which are attached to respective output gate ,下游算子的并发实例数量
	 * @return a (possibly empty) array of integer numbers which indicate the indices of the output channels through
	 * which the record shall be forwarded
	 * 返回该记录应该输出到哪些通道里
	 * 比如返回值是[3,7,5],则将record写出到3个流中，正常情况都是返回1个流,不是所有需求都需要复制流到多个渠道
	 *
	 * 即相当于partition分配器,该数据应该写到哪个partition中
	 *
	 *  比如广播时,要将数据发布到所有子分区内。
	 *  正常partition分布时,只返回一个分区。
	 *
	 *
	 *  该函数表示数据输出到哪个下游分区内
	 *
	 */
	int[] selectChannels(T record, int numChannels);
}
