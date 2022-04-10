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

package org.apache.flink.runtime.iterative.concurrent;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A concurrent data structure that allows the hand-over of an object between a pair of threads.
 * 在多线程中共享同一个对象
 * Broker 代理人
 */
public class Broker<V> {

	//每一个key对应一个公共阻塞队列
	private final ConcurrentMap<String, BlockingQueue<V>> mediations = new ConcurrentHashMap<String, BlockingQueue<V>>();

	/**
	 * Hand in the object to share.
	 * 将obj添加到key对应的公共阻塞队列
	 */
	public void handIn(String key, V obj) {
		if (!retrieveSharedQueue(key).offer(obj)) {
			throw new RuntimeException("Could not register the given element, broker slot is already occupied.");
		}
	}

	/** Blocking retrieval and removal of the object to share.
	 * 获取key队列的一个元素,同时删除该队列
	 **/
	public V getAndRemove(String key) {
		try {
			V objToShare = retrieveSharedQueue(key).take();
			mediations.remove(key);
			return objToShare;
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	/** Blocking retrieval and removal of the object to share.
	 * 移除key队列
	 **/
	public void remove(String key) {
		mediations.remove(key);
	}

	/** Blocking retrieval and removal of the object to share.
	 * 获取key队列的下一个值
	 **/
	public V get(String key) {
		try {
			BlockingQueue<V> queue = retrieveSharedQueue(key);
			V objToShare = queue.take();
			if (!queue.offer(objToShare)) {//怎么又存放进去了? 难道删除用getAndRemove?可能有具体的业务逻辑
				throw new RuntimeException("Error: Concurrent modification of the broker slot for key '" + key + "'.");
			}
			return objToShare;
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Thread-safe call to get a shared {@link BlockingQueue}.
	 * 获取key对应的公共阻塞队列 -- 如果没有,则创建一个
	 */
	private BlockingQueue<V> retrieveSharedQueue(String key) {
		BlockingQueue<V> queue = mediations.get(key);
		if (queue == null) {
			queue = new ArrayBlockingQueue<V>(1);
			BlockingQueue<V> commonQueue = mediations.putIfAbsent(key, queue);
			return commonQueue != null ? commonQueue : queue;
		} else {
			return queue;
		}
	}
}
