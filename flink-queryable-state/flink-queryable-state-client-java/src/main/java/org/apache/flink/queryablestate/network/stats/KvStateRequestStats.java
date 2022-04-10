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

package org.apache.flink.queryablestate.network.stats;

/**
 * Simple statistics for monitoring the state server
 * and the client proxy.
 * 定义统计请求生命周期的动作状态
 */
public interface KvStateRequestStats {

	/**
	 * Reports an active connection.增加连接
	 */
	void reportActiveConnection();

	/**
	 * Reports an inactive connection.减少连接
	 */
	void reportInactiveConnection();

	/**
	 * Reports an incoming request.增加请求
	 */
	void reportRequest();

	/**
	 * Reports a successfully handled request.请求成功
	 *
	 * @param durationTotalMillis Duration of the request (in milliseconds).请求消耗的时间
	 */
	void reportSuccessfulRequest(long durationTotalMillis);

	/**
	 * Reports a failure during a request.请求失败
	 */
	void reportFailedRequest();

}
