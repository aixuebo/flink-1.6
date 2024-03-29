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

package org.apache.flink.api.java.functions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.Partitioner;

/**
 * Partitioner that partitions by id.
 * 分区只和key有关系,与分区数量无关。。注意前提key一定是int类型的,即key已经决定了是哪个分区了
 */
@Internal
public class IdPartitioner implements Partitioner<Integer> {

	private static final long serialVersionUID = -1206233785103357568L;

	@Override
	public int partition(Integer key, int numPartitions) {
		return key;
	}

}
