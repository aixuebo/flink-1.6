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

package org.apache.flink.runtime.rpc;

/**
 * Rpc gateway interface which has to be implemented by Rpc gateways.
 * 网关,设置网关ip与host,所有的服务都可以达到该网关
 */
public interface RpcGateway {

	/**
	 * Returns the fully qualified address under which the associated rpc endpoint is reachable.
	 *
	 * @return Fully qualified (RPC) address under which the associated rpc endpoint is reachable
	 * 提供该服务的网关，
	 * 比如数据需要发送给客户端，那么该地址就是客户端的地址，即接收方的地址。
	 * 如果应用的是服务器，则该地址就是服务器地址，即服务器需要接受客户端的请求。因此目的不同，地址也不同。但总体都是可接收请求的地址
	 */
	String getAddress();

	/**
	 * Returns the fully qualified hostname under which the associated rpc endpoint is reachable.
	 *
	 * @return Fully qualified hostname under which the associated rpc endpoint is reachable
	 */
	String getHostname();
}
