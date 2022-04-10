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

import org.apache.flink.runtime.state.PriorityComparator;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

/**
 * Basic heap-based priority queue for {@link HeapPriorityQueueElement} objects. This heap supports fast deletes
 * because it manages position indexes of the contained {@link HeapPriorityQueueElement}s. The heap implementation is
 * a simple binary tree stored inside an array. Element indexes in the heap array start at 1 instead of 0 to make array
 * index computations a bit simpler in the hot methods. Object identification of remove is based on object identity and
 * not on equals. We use the managed index from {@link HeapPriorityQueueElement} to find an element in the queue
 * array to support fast deletes.
 *
 * <p>Possible future improvements:
 * <ul>
 *  <li>We could also implement shrinking for the heap.</li>
 * </ul>
 *
 * @param <T> type of the contained elements.
 *
 * 添加元素流程:
 * 1.数组扩容,将元素插入到数组最后位置。
 * 2.移动最后一个元素位置,二分法,依次和父节点比较,保证最上面元素是最小的,按照从小到大的顺序,插入到合理的位置上。
 *
 *
 * 一、基于数组实现的优先队列
 * 内部是一个HeapPriorityQueueElement[]。保证数组元素是有顺序的。(添加/删除的时候,通过移动数组,确保依然有序)
 * 通过元素的序号,代表在数组的顺序位置。
 *
 * 提供能力:
 * 1.优先队列,天然的可以从top位置获取数据，并且依次从小到大的迭代元素。
 * 2.通过HeapPriorityQueueElement的序号,可以快速从数组中还原某一个元素。
 *
 *
 * 注意:该优先队列不控制容量,可以无限扩容,即一直add元素,都没有问题(只要内存充足),他只保证了排序,所以元素添加多了,会很卡,排序很耗时
 */
public class HeapPriorityQueue<T extends HeapPriorityQueueElement>
	extends AbstractHeapPriorityQueue<T> {

	/**
	 * The index of the head element in the array that represents the heap.
	 * 头部元素位置
	 */
	private static final int QUEUE_HEAD_INDEX = 1;

	/**
	 * Comparator for the priority of contained elements.
	 * 如何比较元素
	 */
	@Nonnull
	protected final PriorityComparator<T> elementPriorityComparator;

	/**
	 * Creates an empty {@link HeapPriorityQueue} with the requested initial capacity.
	 *
	 * @param elementPriorityComparator comparator for the priority of contained elements.
	 * @param minimumCapacity the minimum and initial capacity of this priority queue.
	 */
	@SuppressWarnings("unchecked")
	public HeapPriorityQueue(
		@Nonnull PriorityComparator<T> elementPriorityComparator,
		@Nonnegative int minimumCapacity) {
		super(minimumCapacity);
		this.elementPriorityComparator = elementPriorityComparator;
	}

	public void adjustModifiedElement(@Nonnull T element) {
		final int elementIndex = element.getInternalIndex();
		if (element == queue[elementIndex]) {
			adjustElementAtIndex(element, elementIndex);
		}
	}

	//头部元素索引,即最小的元素索引
	@Override
	protected int getHeadElementIndex() {
		return QUEUE_HEAD_INDEX;
	}

	@Override
	protected void addInternal(@Nonnull T element) {
		final int newSize = increaseSizeByOne();//一共有多少个元素
		moveElementToIdx(element, newSize);//将元素插入到最后一个位置
		siftUp(newSize);//移动位置,进行排序
	}

	@Override
	protected T removeInternal(int removeIdx) {
		T[] heap = this.queue;
		T removedValue = heap[removeIdx];//获取堆内的元素

		assert removedValue.getInternalIndex() == removeIdx;//确保元素的位置与参数相同

		final int oldSize = size;

		if (removeIdx != oldSize) {//说明不是移除的最后一个元素
			T element = heap[oldSize];
			moveElementToIdx(element, removeIdx);
			adjustElementAtIndex(element, removeIdx);
		}

		heap[oldSize] = null;//设置最后一个元素值为null

		--size;
		return removedValue;
	}

	private void adjustElementAtIndex(T element, int index) {
		siftDown(index);
		if (queue[index] == element) {
			siftUp(index);
		}
	}

	//从idx最后一个位置,开始比较,将最小的向上滚动
	private void siftUp(int idx) {
		final T[] heap = this.queue;
		final T currentElement = heap[idx];
		int parentIdx = idx >>> 1;

		while (parentIdx > 0 && isElementPriorityLessThen(currentElement, heap[parentIdx])) {
			moveElementToIdx(heap[parentIdx], idx);
			idx = parentIdx;
			parentIdx >>>= 1;
		}

		moveElementToIdx(currentElement, idx);
	}

	private void siftDown(int idx) {
		final T[] heap = this.queue;
		final int heapSize = this.size;

		final T currentElement = heap[idx];
		int firstChildIdx = idx << 1;
		int secondChildIdx = firstChildIdx + 1;

		if (isElementIndexValid(secondChildIdx, heapSize) &&
			isElementPriorityLessThen(heap[secondChildIdx], heap[firstChildIdx])) {
			firstChildIdx = secondChildIdx;
		}

		while (isElementIndexValid(firstChildIdx, heapSize) &&
			isElementPriorityLessThen(heap[firstChildIdx], currentElement)) {
			moveElementToIdx(heap[firstChildIdx], idx);
			idx = firstChildIdx;
			firstChildIdx = idx << 1;
			secondChildIdx = firstChildIdx + 1;

			if (isElementIndexValid(secondChildIdx, heapSize) &&
				isElementPriorityLessThen(heap[secondChildIdx], heap[firstChildIdx])) {
				firstChildIdx = secondChildIdx;
			}
		}

		moveElementToIdx(currentElement, idx);
	}

	//当前元素索引有效
	private boolean isElementIndexValid(int elementIndex, int heapSize) {
		return elementIndex <= heapSize;
	}

	//元素比较
	private boolean isElementPriorityLessThen(T a, T b) {
		return elementPriorityComparator.comparePriority(a, b) < 0;
	}

	//返回真实的元素数量size
	private int increaseSizeByOne() {
		final int oldArraySize = queue.length;//容器总大小
		final int minRequiredNewSize = ++size;//元素+1
		if (minRequiredNewSize >= oldArraySize) {//元素+1后  是否超过了容器总大小
			final int grow = (oldArraySize < 64) ? oldArraySize + 2 : oldArraySize >> 1; //容器超过64个内容,则每次扩大一倍数据量
			resizeQueueArray(oldArraySize + grow, minRequiredNewSize);//仅仅数组扩容
		}
		// TODO implement shrinking as well?
		return minRequiredNewSize;
	}
}
