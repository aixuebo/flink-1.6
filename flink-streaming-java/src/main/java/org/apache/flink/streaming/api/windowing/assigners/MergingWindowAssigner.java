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

package org.apache.flink.streaming.api.windowing.assigners;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.windowing.windows.Window;

import java.util.Collection;

/**
 * A {@code WindowAssigner} that can merge windows.
 *
 * @param <T> The type of elements that this WindowAssigner can assign windows to.
 * @param <W> The type of {@code Window} that this assigner assigns.
 *
 * 参考 EventTimeSessionWindows
 *
 * 一旦触发合并,则计算List<Window>如何拆分与合并。最后组成List<List<Window>>形式，即第一个list表示拆分独立的窗口，第二个list表示每一个独立窗口中包含了哪些窗口的merge。
 */
@PublicEvolving
public abstract class MergingWindowAssigner<T, W extends Window> extends WindowAssigner<T, W> {
	private static final long serialVersionUID = 1L;

	/**
	 * Determines which windows (if any) should be merged.
	 *
	 * @param windows The window candidates.
	 * @param callback A callback that can be invoked to signal which windows should be merged.
	 *
	 * 触发List<Window>的合并/拆分操作.比如List的size=10,因此合并后的结果一定<=10,即一定有一些window是可以merge合并的。假设最终是4个。
	 * 因此这4个真实的独立窗口会触发MergeCallback回调。即循环4次，每次合并归属的可以合并的window集合
	 */
	public abstract void mergeWindows(Collection<W> windows, MergeCallback<W> callback);

	/**
	 * Callback to be used in {@link #mergeWindows(Collection, MergeCallback)} for specifying which
	 * windows should be merged.
	 */
	public interface MergeCallback<W> {

		/**
		 * Specifies that the given windows should be merged into the result window.
		 *
		 * @param toBeMerged The list of windows that should be merged into one window. 同一个交集的window窗口集合
		 * @param mergeResult The resulting merged window.覆盖所有交集的窗口最大的范围区间
		 */
		void merge(Collection<W> toBeMerged, W mergeResult);
	}
}
