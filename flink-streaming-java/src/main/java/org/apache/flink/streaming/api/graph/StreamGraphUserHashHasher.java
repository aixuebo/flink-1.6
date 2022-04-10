/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.graph;

import org.apache.flink.util.StringUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * StreamGraphHasher that works with user provided hashes. This is useful in case we want to set (alternative) hashes
 * explicitly,
 * 用于用户自定义明确的可控选择操作名称。
 * 即用为为每一个操作自定义名称，用于在监控页面看到自己的操作执行内容
 * e.g. to provide a way of manual backwards compatibility between versions when the mechanism of generating
 * hashes has changed in an incompatible way.
 *
 如果用户对节点指定了一个散列值，则基于用户指定的值能够产生一个长度为 16 的字节数组。如果用户没有指定，则根据当前节点所处的位置，产生一个散列值。

 为每个operator生成hash的原因

 Flink 任务失败的时候，各个 operator 是能够从 checkpoint 中恢复到失败之前的状态的，恢复的时候是依据 JobVertexID（hash 值)进行状态恢复的。相同的任务在恢复的时候要求 operator 的 hash 值不变，因此能够获取对应的状态。

 *
 */
public class StreamGraphUserHashHasher implements StreamGraphHasher {

	//返回每一个操作id,自定义的hash二进制key
	@Override
	public Map<Integer, byte[]> traverseStreamGraphAndGenerateHashes(StreamGraph streamGraph) {
		HashMap<Integer, byte[]> hashResult = new HashMap<>();
		for (StreamNode streamNode : streamGraph.getStreamNodes()) {

			String userHash = streamNode.getUserHash();

			if (null != userHash) {
				hashResult.put(streamNode.getId(), StringUtils.hexStringToByte(userHash));
			}
		}

		return hashResult;
	}
}
