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

package org.apache.flink.core.io;


import org.apache.flink.annotation.PublicEvolving;

/**
 * An input split assigner distributes the {@link InputSplit}s among the instances on which a
 * data source exists.
 * 为节点分配一个待处理的数据块
 *
 * jobManager知道要加载的所有数据源List<InputSplit>,task节点要执行的时候，只需要请求jobManager,
 * jobManager知道task所在的节点host、以及执行第几个task,
 * 根据这两个信息，随机或者策略的方式分配一个InputSplit给task去执行。
 */
@PublicEvolving
public interface InputSplitAssigner {

	/**
	 * Returns the next input split that shall be consumed. The consumer's host is passed as a parameter
	 * to allow localized assignments.
	 * 传递请求数据块节点id,目的是分配本地化的数据块,属于优化范畴
	 * @param host The host address of split requesting task.请求数据块的节点
	 * @param taskId The id of the split requesting task.请求数据块的任务id
	 * @return the next input split to be consumed, or <code>null</code> if no more splits remain.
	 * 返回要去处理的数据块
	 */
	InputSplit getNextInputSplit(String host, int taskId);

}
