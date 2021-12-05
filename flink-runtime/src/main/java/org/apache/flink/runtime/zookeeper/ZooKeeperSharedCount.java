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

package org.apache.flink.runtime.zookeeper;

import org.apache.curator.framework.recipes.shared.SharedCount;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

/**
 * Wrapper class for a {@link SharedCount} so that we don't expose a curator dependency in our
 * internal APIs. Such an exposure is problematic due to the relocation of curator.
 *
 * zookeeper存储int值
 */
public class ZooKeeperSharedCount {

	private final SharedCount sharedCount;

	public ZooKeeperSharedCount(SharedCount sharedCount) {
		this.sharedCount = Preconditions.checkNotNull(sharedCount);
	}

	public void start() throws Exception {
		sharedCount.start();
	}

	public void close() throws IOException {
		sharedCount.close();
	}

	//获取当前value+version
	public ZooKeeperVersionedValue<Integer> getVersionedValue() {
		return new ZooKeeperVersionedValue<>(sharedCount.getVersionedValue());
	}

	//基于当前值的value+version，去设置新的值。当前值是int类型
	public boolean trySetCount(ZooKeeperVersionedValue<Integer> previous, int newCount) throws Exception {
		return sharedCount.trySetCount(previous.getVersionedValue(), newCount);
	}
}
