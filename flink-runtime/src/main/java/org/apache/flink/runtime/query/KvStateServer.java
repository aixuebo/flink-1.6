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

package org.apache.flink.runtime.query;

import java.net.InetSocketAddress;

/**
 * An interface for the Queryable State Server running on each Task Manager in the cluster.
 * This server is responsible for serving requests coming from the {@link KvStateClientProxy
 * Queryable State Proxy} and requesting <b>locally</b> stored state.
 *
 * 当用户在job中定义了queryable state之后，就可以在外部，通过QueryableStateClient，通过job id, state name, key来查询所对应的状态的实时的值。
 */
public interface KvStateServer {

	/**
	 * Returns the {@link InetSocketAddress address} the server is listening to.
	 * @return Server address.
	 * 服务器地址
	 */
	InetSocketAddress getServerAddress();


	/** Starts the server. */
	void start() throws Throwable;

	/** Shuts down the server and all related thread pools. */
	void shutdown();
}
