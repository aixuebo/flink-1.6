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

import org.apache.flink.annotation.Internal;

/**
 * Interface for objects that can be managed by a {@link HeapPriorityQueue}. Such an object can only be contained in at
 * most one {@link HeapPriorityQueue} at a time.
 * 表示一个对象，存在 内部堆实现的优先队列中。
 *
 * 堆内的对象有一个特性,可以通过索引快速定位到对象
 *
 *
 * 查看readme
 */
@Internal
public interface HeapPriorityQueueElement {

	/**
	 * The index that indicates that a {@link HeapPriorityQueueElement} object is not contained in and managed by any
	 * {@link HeapPriorityQueue}. We do not strictly enforce that internal indexes must be reset to this value when
	 * elements are removed from a {@link HeapPriorityQueue}.
	 * 表示 不包含的索引值
	 */
	int NOT_CONTAINED = Integer.MIN_VALUE;//没有实现堆内的索引查询元素功能

	/**
	 * Returns the current index of this object in the internal array of {@link HeapPriorityQueue}.
	 * 获取在队列中 该元素的索引
	 */
	int getInternalIndex();

	/**
	 * Sets the current index of this object in the {@link HeapPriorityQueue} and should only be called by the owning
	 * {@link HeapPriorityQueue}.
	 *
	 * @param newIndex the new index in the timer heap.
	 * 元素的索引位置
	 */
	void setInternalIndex(int newIndex);
}
