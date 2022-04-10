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

package org.apache.flink.runtime.state.heap;

import org.apache.flink.runtime.state.KeyExtractorFunction;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.PriorityComparator;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A heap-based priority queue with set semantics, based on {@link HeapPriorityQueue}. The heap is supported by hash
 * set for fast contains (de-duplication) and deletes. Object identification happens based on {@link #equals(Object)}.
 *
 * <p>Possible future improvements:
 * <ul>
 *  <li>We could also implement shrinking for the heap and the deduplication set.</li>
 *  <li>We could replace the deduplication maps with more efficient custom implementations. In particular, a hash set
 * would be enough if it could return existing elements on unsuccessful adding, etc..</li>
 * </ul>
 *
 * @param <T> type of the contained elements.
 *
 * 可以存放不重复的元素的优先队列
 *
 * 由于堆内存HeapPriorityQueue本身只是一个数组,确保了每一个元素添加后是有顺序的,但不能保证不重复。
 * 因此扩展,先将数据存放到内部的map中,如果添加成功,说明数据不存在,因此可以放到优先队列里
 *
 * 注意:这里他会根据每一个key生成一个map,确保每一个key的数据不会重复,但多个key之间会有重复的，因此像是应用在merge操作
 *
 *
 * 同一个节点 有 多个partition分区结果，所以对应多个HeapPriorityQueue队列。但由于key肯定不会重复分布在多个partition中，因此存储上使用同一个HeapPriorityQueue存储所有partition的key内容。
 * 但在hash中,进行拆分
 */
public class HeapPriorityQueueSet<T extends HeapPriorityQueueElement>
	extends HeapPriorityQueue<T>
	implements KeyGroupedInternalPriorityQueue<T> {

	/**
	 * Function to extract the key from contained elements.
	 * 如何提取key对象值,即T转成Object的key对象
	 */
	private final KeyExtractorFunction<T> keyExtractor;

	/**
	 * This array contains one hash set per key-group. The sets are used for fast de-duplication and deletes of elements.
	 * 插入和删除前,先在map中尝试,如果成功,说明允许插入或者删除
	 *
	 * 比如KeyGroupRange范围内有100个组,则有100个HashMap
	 */
	private final HashMap<T, T>[] deduplicationMapsByKeyGroup;//存储每一个partition对应的key集合

	/**
	 * The key-group range of elements that are managed by this queue.
	 * key的范围 -- 即分成了多少个组
	 */
	private final KeyGroupRange keyGroupRange;//存储该队列的key所在范围

	/**
	 * The total number of key-groups of the job.  总partition数量
	 */
	private final int totalNumberOfKeyGroups;

	/**
	 * Creates an empty {@link HeapPriorityQueueSet} with the requested initial capacity.
	 *
	 * @param elementPriorityComparator comparator for the priority of contained elements.//如何比较元素
	 * @param keyExtractor function to extract a key from the contained elements.//如何提取key
	 * @param minimumCapacity the minimum and initial capacity of this priority queue.初始化队列大小,后期可以扩容的
	 * @param keyGroupRange the key-group range of the elements in this set.分组数量
	 * @param totalNumberOfKeyGroups the total number of key-groups of the job.总partition数量
	 */
	@SuppressWarnings("unchecked")
	public HeapPriorityQueueSet(
		@Nonnull PriorityComparator<T> elementPriorityComparator,//如何比较
		@Nonnull KeyExtractorFunction<T> keyExtractor,//如何从元素中提取key,因为要通过key定位到他所在的在的小组
		@Nonnegative int minimumCapacity,
		@Nonnull KeyGroupRange keyGroupRange,
		@Nonnegative int totalNumberOfKeyGroups) {

		super(elementPriorityComparator, minimumCapacity);

		this.keyExtractor = keyExtractor;

		this.totalNumberOfKeyGroups = totalNumberOfKeyGroups;
		this.keyGroupRange = keyGroupRange;

		final int keyGroupsInLocalRange = keyGroupRange.getNumberOfKeyGroups();//允许存放的key范围数量
		final int deduplicationSetSize = 1 + minimumCapacity / keyGroupsInLocalRange;//初始化的容量意义不大,可以忽略 --- 初始化hashMap的size,建议每一个key内存储多少个元素
		this.deduplicationMapsByKeyGroup = new HashMap[keyGroupsInLocalRange];
		for (int i = 0; i < keyGroupsInLocalRange; ++i) {
			deduplicationMapsByKeyGroup[i] = new HashMap<>(deduplicationSetSize);
		}
	}

	@Override
	@Nullable
	public T poll() {
		final T toRemove = super.poll();
		return toRemove != null ? getDedupMapForElement(toRemove).remove(toRemove) : null;//hash桶内元素也跟着删除掉
	}

	/**
	 * Adds the element to the queue. In contrast to the superclass and to maintain set semantics, this happens only if
	 * no such element is already contained (determined by {@link #equals(Object)}).
	 *
	 * @return <code>true</code> if the operation changed the head element or if is it unclear if the head element changed.
	 * Only returns <code>false</code> iff the head element was not changed by this operation.
	 */
	@Override
	public boolean add(@Nonnull T element) {
		//将元素先编码成key序号,然后找到存放该key的hash桶,然后找到该元素
		//如果元素不存在,则调用上层添加
		return getDedupMapForElement(element).putIfAbsent(element, element) == null && super.add(element);
	}

	/**
	 * In contrast to the superclass and to maintain set semantics, removal here is based on comparing the given element
	 * via {@link #equals(Object)}.
	 *
	 * @return <code>true</code> if the operation changed the head element or if is it unclear if the head element changed.
	 * Only returns <code>false</code> iff the head element was not changed by this operation.
	 */
	@Override
	public boolean remove(@Nonnull T toRemove) {
		T storedElement = getDedupMapForElement(toRemove).remove(toRemove);//将元素先编码成key序号,然后找到存放该key的hash桶,然后找到该元素
		return storedElement != null && super.remove(storedElement);//说明元素确实存在,因此调用上层队列直接删除操作
	}

	@Override
	public void clear() {
		super.clear();
		for (HashMap<?, ?> elementHashMap : deduplicationMapsByKeyGroup) {
			elementHashMap.clear();
		}
	}

	/**
	 * @param keyGroupId key应该归属在哪个分桶内
	 * @return 返回key应该存放的hash分桶
	 */
	private HashMap<T, T> getDedupMapForKeyGroup(@Nonnegative int keyGroupId) {
		return deduplicationMapsByKeyGroup[globalKeyGroupToLocalIndex(keyGroupId)];
	}

	/**
	 * 将元素先编码成key序号,然后找到存放该key的hash桶
	 * @param element
	 * @return
	 */
	private HashMap<T, T> getDedupMapForElement(T element) {
		//全局内,key应该分配到哪个group组序号
		int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(
			keyExtractor.extractKeyFromElement(element),//提取key
			totalNumberOfKeyGroups);
		return getDedupMapForKeyGroup(keyGroup);
	}

	//确保keyGroup一定在keyGroupRange内。
	//返回keyGroup在keyGroupRange属于第几个小组
	private int globalKeyGroupToLocalIndex(int keyGroup) {
		checkArgument(keyGroupRange.contains(keyGroup));
		return keyGroup - keyGroupRange.getStartKeyGroup();
	}

	/**
	 * 返回key存在的hash分桶内所有的key元素
	 * @param keyGroupId
	 * @return
	 */
	@Nonnull
	@Override
	public Set<T> getSubsetForKeyGroup(int keyGroupId) {
		return getDedupMapForKeyGroup(keyGroupId).keySet();
	}
}
