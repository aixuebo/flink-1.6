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

package org.apache.flink.streaming.api.transformations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import java.util.Collection;
import java.util.List;

/**
 * This transformation represents a change of partitioning of the input elements.
 *
 * <p>This does not create a physical operation, it only affects how upstream operations are
 * connected to downstream operations.
 *
 * @param <T> The type of the elements that result from this {@code PartitionTransformation}
 * 更改流元素的分区，输出类型依然是原始流类型，因此不需要操作，只需要一个分区算法对象即可
 *
 * 说明需要shuffle处理的流转换操作,相当于在做repartition操作
 *
 */
@Internal
public class PartitionTransformation<T> extends StreamTransformation<T> {

	private final StreamTransformation<T> input;//原始流输入数据
	private final StreamPartitioner<T> partitioner;//如何拆分成key,按照key分发数据进行shuffle,即如何做repartition操作

	/**
	 * Creates a new {@code PartitionTransformation} from the given input and
	 * {@link StreamPartitioner}.
	 *
	 * @param input The input {@code StreamTransformation}
	 * @param partitioner The {@code StreamPartitioner}
	 */
	public PartitionTransformation(StreamTransformation<T> input, StreamPartitioner<T> partitioner) {
		super("Partition", input.getOutputType(), input.getParallelism());//只是重新分区,流依然还是原来的流类型,因此类型不变
		this.input = input;
		this.partitioner = partitioner;
	}

	/**
	 * Returns the input {@code StreamTransformation} of this {@code SinkTransformation}.
	 */
	public StreamTransformation<T> getInput() {
		return input;
	}

	/**
	 * Returns the {@code StreamPartitioner} that must be used for partitioning the elements
	 * of the input {@code StreamTransformation}.
	 */
	public StreamPartitioner<T> getPartitioner() {
		return partitioner;
	}

	@Override
	public Collection<StreamTransformation<?>> getTransitivePredecessors() {
		List<StreamTransformation<?>> result = Lists.newArrayList();
		result.add(this);
		result.addAll(input.getTransitivePredecessors());
		return result;
	}

	@Override
	public final void setChainingStrategy(ChainingStrategy strategy) {
		throw new UnsupportedOperationException("Cannot set chaining strategy on Union Transformation.");
	}
}
