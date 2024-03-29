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

package org.apache.flink.runtime.io.network;

import org.apache.flink.runtime.io.network.netty.PartitionRequestClient;
import org.apache.flink.runtime.io.network.partition.ResultPartitionProvider;

import java.io.IOException;

/**
 * The connection manager manages physical connections for the (logical) remote
 * input channels at runtime.
 *
 * 一个物理的网络连接
 */
public interface ConnectionManager {

	void start(ResultPartitionProvider partitionProvider,
				TaskEventDispatcher taskEventDispatcher) throws IOException;

	/**
	 * Creates a {@link PartitionRequestClient} instance for the given {@link ConnectionID}.
	 */
	PartitionRequestClient createPartitionRequestClient(ConnectionID connectionId) throws IOException, InterruptedException;

	/**
	 * Closes opened ChannelConnections in case of a resource release.
	 */
	void closeOpenChannelConnections(ConnectionID connectionId);

	int getNumberOfActiveConnections();

	//返回服务端端口
	int getDataPort();

	//关闭连接
	void shutdown() throws IOException;

}
