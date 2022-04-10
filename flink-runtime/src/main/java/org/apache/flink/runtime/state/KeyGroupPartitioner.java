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

package org.apache.flink.runtime.state;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;

/**
 * Class that contains the base algorithm for partitioning data into key-groups. This algorithm currently works
 * with two array (input, output) for optimal algorithmic complexity. Notice that this could also be implemented over a
 * single array, using some cuckoo-hashing-style element replacement. This would have worse algorithmic complexity but
 * better space efficiency. We currently prefer the trade-off in favor of better algorithmic complexity.
 *
 * @param <T> type of the partitioned elements.
 *
 * 将元素存储在内存中，由于内存中存储的元素归属于多个partition,所以最终结果是按照partition的顺序,把元素一个个存储在内存数组中。
 *
 * 因此 当需要输出某一个分区的数据时，只需要判断好该分区在内存数组的顺序区间，就可以获取该分区的数据了,输出到指定的输出流中
 */
public class KeyGroupPartitioner<T> {

	/**
	 * The input data for the partitioning. All elements to consider must be densely in the index interval
	 * [0, {@link #numberOfElements}[, without null values.
	 * 内存中存储的原始数据内容
	 */
	@Nonnull
	protected final T[] partitioningSource;

	/**
	 * The output array for the partitioning. The size must be {@link #numberOfElements} (or bigger).
	 * 按照partition顺序,将元素依次写到字节数组中
	 */
	@Nonnull
	protected final T[] partitioningDestination;

	/** Total number of input elements. 内存中存储的元素总数*/
	@Nonnegative
	protected final int numberOfElements;

	/** The total number of key-groups in the job. 一共多少个分区*/
	@Nonnegative
	protected final int totalKeyGroups;

	/** The key-group range for the input data, covered in this partitioning. */
	@Nonnull
	protected final KeyGroupRange keyGroupRange;

	/**
	 * This bookkeeping array is used to count the elements in each key-group. In a second step, it is transformed into
	 * a histogram by accumulation.
	 * 保存每一个partition分区有多少个元素 --- 每一个partition的数量都是累计值,一个比一个大
	 */
	@Nonnull
	protected final int[] counterHistogram;

	/**
	 * This is a helper array that caches the key-group for each element, so we do not have to compute them twice.所以我们没有计算两次
	 * 设置每一个元素对应的第几个partition
	 */
	@Nonnull
	protected final int[] elementKeyGroups;

	/** Cached value of keyGroupRange#firstKeyGroup. task节点上第一个partition的id */
	@Nonnegative
	protected final int firstKeyGroup;

	/** Function to extract the key from a given element. */
	@Nonnull
	protected final KeyExtractorFunction<T> keyExtractorFunction;

	/** Function to write an element to a {@link DataOutputView}.
	 * 如何输出元素
	 **/
	@Nonnull
	protected final ElementWriterFunction<T> elementWriterFunction;

	/** Cached result. */
	@Nullable
	protected StateSnapshot.StateKeyGroupWriter computedResult;

	/**
	 * Creates a new {@link KeyGroupPartitioner}.
	 *
	 * @param partitioningSource the input for the partitioning. All elements must be densely packed in the index
	 *                              interval [0, {@link #numberOfElements}[, without null values.
	 * @param numberOfElements the number of elements to consider from the input, starting at input index 0.
	 * @param partitioningDestination the output of the partitioning. Must have capacity of at least numberOfElements.
	 * @param keyGroupRange the key-group range of the data that will be partitioned by this instance.
	 * @param totalKeyGroups the total number of key groups in the job.
	 * @param keyExtractorFunction this function extracts the partition key from an element.
	 */
	public KeyGroupPartitioner(
		@Nonnull T[] partitioningSource,//数据源--内存中存储的数组对象
		@Nonnegative int numberOfElements,//元素数量
		@Nonnull T[] partitioningDestination,//对partitioningSource进行排序,排序后的输出数组
		@Nonnull KeyGroupRange keyGroupRange,
		@Nonnegative int totalKeyGroups,//task节点上的partition数量
		@Nonnull KeyExtractorFunction<T> keyExtractorFunction,
		@Nonnull ElementWriterFunction<T> elementWriterFunction) {//如何对数据处理,输出输出流中

		Preconditions.checkState(partitioningSource != partitioningDestination);
		Preconditions.checkState(partitioningSource.length >= numberOfElements);
		Preconditions.checkState(partitioningDestination.length >= numberOfElements);

		this.partitioningSource = partitioningSource;
		this.partitioningDestination = partitioningDestination;
		this.numberOfElements = numberOfElements;
		this.keyGroupRange = keyGroupRange;
		this.totalKeyGroups = totalKeyGroups;
		this.keyExtractorFunction = keyExtractorFunction;
		this.elementWriterFunction = elementWriterFunction;
		this.firstKeyGroup = keyGroupRange.getStartKeyGroup();
		this.elementKeyGroups = new int[numberOfElements];//设置每一个元素对应的第几个partition
		this.counterHistogram = new int[keyGroupRange.getNumberOfKeyGroups()];//每一个partition的累计数量
		this.computedResult = null;
	}

	/**
	 * Partitions the data into key-groups and returns the result via {@link PartitioningResult}.
	 * 返回partition输出对象
	 *
	 * 相当于init方法,对数据源进行处理
	 */
	public StateSnapshot.StateKeyGroupWriter partitionByKeyGroup() {
		if (computedResult == null) {
			reportAllElementKeyGroups();//计算每一个partition的元素数量
			int outputNumberOfElements = buildHistogramByAccumulatingCounts();//计算一共多少个元素数量,以及计算partition累计数量
			executePartitioning(outputNumberOfElements);
		}
		return computedResult;
	}

	/**
	 * This method iterates over the input data and reports the key-group for each element.
	 * 设置每一个元素归属的partition信息
	 */
	protected void reportAllElementKeyGroups() {

		Preconditions.checkState(partitioningSource.length >= numberOfElements);

		for (int i = 0; i < numberOfElements; ++i) {
			int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(
				keyExtractorFunction.extractKeyFromElement(partitioningSource[i]), totalKeyGroups);//key归属第几个partition
			reportKeyGroupOfElementAtIndex(i, keyGroup);
		}
	}

	/**
	 * This method reports in the bookkeeping data that the element at the given index belongs to the given key-group.
	 * 报告每一个元素归属第几个partition
	 * index表示第几个元素,keyGroup表示归属第几个partition
	 */
	protected void reportKeyGroupOfElementAtIndex(int index, int keyGroup) {
		final int keyGroupIndex = keyGroup - firstKeyGroup;
		elementKeyGroups[index] = keyGroupIndex;//设置每一个元素对应的第几个partition
		++counterHistogram[keyGroupIndex];//保存每一个partition分区有多少个元素
	}

	/**
	 * This method creates a histogram from the counts per key-group in {@link #counterHistogram}.
	 * 计算累计partition的数量
	 */
	private int buildHistogramByAccumulatingCounts() {
		int sum = 0;
		for (int i = 0; i < counterHistogram.length; ++i) {//循环每一个partition
			int currentSlotValue = counterHistogram[i];//该partition上有多少个元素
			counterHistogram[i] = sum;//计算累计元素实例
			sum += currentSlotValue;
		}
		return sum;
	}

	//参数是总共有多少个元素数量
	private void executePartitioning(int outputNumberOfElements) {

		// We repartition the entries by their pre-computed key-groups, using the histogram values as write indexes
		for (int inIdx = 0; inIdx < outputNumberOfElements; ++inIdx) { //循环每一个元素
			int effectiveKgIdx = elementKeyGroups[inIdx];//归属第几个partition
			int outIdx = counterHistogram[effectiveKgIdx]++;//比如归属第20个parition,而第20个partition的累计是是80,说明该值排序在第81个位置
			partitioningDestination[outIdx] = partitioningSource[inIdx];//数据源应该放到结果目标的哪个位置上
		}

		this.computedResult = new PartitioningResult<>(
			elementWriterFunction,//如何输出数据
			firstKeyGroup,
			counterHistogram,//partition累计值
			partitioningDestination);//按照partition顺序存储的所有元素值
	}

	/**
	 * This represents the result of key-group partitioning. The data in {@link #partitionedElements} is partitioned
	 * w.r.t. {@link KeyGroupPartitioner#keyGroupRange}.
	 */
	private static class PartitioningResult<T> implements StateSnapshot.StateKeyGroupWriter {

		/**
		 * Function to write one element to a {@link DataOutputView}.
		 * 输入输出
		 */
		@Nonnull
		private final ElementWriterFunction<T> elementWriterFunction;

		/**
		 * The exclusive-end-offsets for all key-groups of the covered range for the partitioning. Exclusive-end-offset
		 * for key-group n is under keyGroupOffsets[n - firstKeyGroup].
		 * 设置每一个分区的结束位置
		 * 比如 7 10 15,表示7第一个分区结束，10结束第二个分区 15结束第三个分区，即第一个分区内容是7个，第二个分区内容是3个，第三个分区5个
		 */
		@Nonnull
		private final int[] keyGroupOffsets;

		/**
		 * Array with elements that are partitioned w.r.t. the covered key-group range. The start offset for each
		 * key-group is in {@link #keyGroupOffsets}.
		 * 按照分区顺序,存储所有的分区数据
		 */
		@Nonnull
		private final T[] partitionedElements;

		/**
		 * The first key-group of the range covered in the partitioning.
		 */
		@Nonnegative
		private final int firstKeyGroup;

		PartitioningResult(
			@Nonnull ElementWriterFunction<T> elementWriterFunction,
			@Nonnegative int firstKeyGroup,
			@Nonnull int[] keyGroupEndOffsets,
			@Nonnull T[] partitionedElements) {
			this.elementWriterFunction = elementWriterFunction;
			this.firstKeyGroup = firstKeyGroup;
			this.keyGroupOffsets = keyGroupEndOffsets;
			this.partitionedElements = partitionedElements;
		}

		@Nonnegative
		private int getKeyGroupStartOffsetInclusive(int keyGroup) {
			int idx = keyGroup - firstKeyGroup - 1;
			return idx < 0 ? 0 : keyGroupOffsets[idx];
		}

		@Nonnegative
		private int getKeyGroupEndOffsetExclusive(int keyGroup) {
			return keyGroupOffsets[keyGroup - firstKeyGroup];
		}

		//输出属于该分区的数据内容
		@Override
		public void writeStateInKeyGroup(@Nonnull DataOutputView dov, int keyGroupId) throws IOException {

			int startOffset = getKeyGroupStartOffsetInclusive(keyGroupId);
			int endOffset = getKeyGroupEndOffsetExclusive(keyGroupId);

			// write number of mappings in key-group
			dov.writeInt(endOffset - startOffset);

			//找到该分区的数据范围,输出分区的数据内容
			// write mappings
			for (int i = startOffset; i < endOffset; ++i) {
				elementWriterFunction.writeElement(partitionedElements[i], dov);
			}
		}
	}

	//内存中反序列化StateSnapshot
	public static <T> StateSnapshotKeyGroupReader createKeyGroupPartitionReader(
			@Nonnull ElementReaderFunction<T> readerFunction,//如何提取命名空间+key+value三元组
			@Nonnull KeyGroupElementsConsumer<T> elementConsumer) {//如何消费三元组,存储到内存
		return new PartitioningResultKeyGroupReader<>(readerFunction, elementConsumer);
	}

	/**
	 * General algorithm to read key-grouped state that was written from a {@link PartitioningResult}.
	 *
	 * @param <T> type of the elements to read.
	 *
	 * 读取某一个partition分组下的StateSnapshot
	 */
	private static class PartitioningResultKeyGroupReader<T> implements StateSnapshotKeyGroupReader {

		@Nonnull
		private final ElementReaderFunction<T> readerFunction;//如何反序列化byte[命名空间] + byte[key] + byte[value]三元组

		@Nonnull
		private final KeyGroupElementsConsumer<T> elementConsumer;//如何处理一条key=value数据

		public PartitioningResultKeyGroupReader(
			@Nonnull ElementReaderFunction<T> readerFunction,
			@Nonnull KeyGroupElementsConsumer<T> elementConsumer) {

			this.readerFunction = readerFunction;
			this.elementConsumer = elementConsumer;
		}

		/**
		 * @param in byte[命名空间] + byte[key] + byte[value]
		 * @param keyGroupId the key-group to write
		 * @throws IOException
		 * 读取某一个partition分组下的StateSnapshot
		 */
		@Override
		public void readMappingsInKeyGroup(@Nonnull DataInputView in, @Nonnegative int keyGroupId) throws IOException {
			int numElements = in.readInt();//多少条数据
			for (int i = 0; i < numElements; i++) {
				T element = readerFunction.readElement(in);//读取一个元素
				elementConsumer.consume(element, keyGroupId);
			}
		}
	}

	/**
	 * This functional interface defines how one element is written to a {@link DataOutputView}.
	 *
	 * @param <T> type of the written elements.
	 */
	@FunctionalInterface
	public interface ElementWriterFunction<T> {

		/**
		 * This method defines how to write a single element to the output.
		 *
		 * @param element the element to be written.
		 * @param dov     the output view to write the element.
		 * @throws IOException on write-related problems.
		 * 输出byte[命名空间] + byte[key] + byte[value]三元组
		 */
		void writeElement(@Nonnull T element, @Nonnull DataOutputView dov) throws IOException;
	}

	/**
	 * This functional interface defines how one element is read from a {@link DataInputView}.
	 *
	 * @param <T> type of the read elements.
	 */
	@FunctionalInterface
	public interface ElementReaderFunction<T> {

		@Nonnull
		T readElement(@Nonnull DataInputView div) throws IOException; //T是byte[命名空间] + byte[key] + byte[value],如果从输入中反序列化三元组
	}

	/**
	 * Functional interface to consume elements from a key group.
	 *
	 * @param <T> type of the consumed elements. byte[命名空间] + byte[key] + byte[value]三元组
	 *
	 */
	@FunctionalInterface
	public interface KeyGroupElementsConsumer<T> {
		//消费一个元素,包含元素的对象 以及 元素归属第几个partition
		void consume(@Nonnull T element, @Nonnegative int keyGroupId) throws IOException;
	}
}
