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

package org.apache.flink.runtime.rpc.messages;

import javax.annotation.Nonnull;

import java.io.Serializable;

/**
 * Handshake message between rpc endpoints. This message can be used
 * to verify compatibility between different endpoints.
 * 请求远程服务哪个网关对象 --- 用于判定是否支持class这个接口
 */
public class RemoteHandshakeMessage implements Serializable {

	private static final long serialVersionUID = -7150082246232019027L;

	@Nonnull
	private final Class<?> rpcGateway;

	@Nonnull
	private final int version;

	public RemoteHandshakeMessage(@Nonnull Class<?> rpcGateway, @Nonnull int version) {
		this.rpcGateway = rpcGateway;
		this.version = version;
	}

	@Nonnull
	public Class<?> getRpcGateway() {
		return rpcGateway;
	}

	@Nonnull
	public int getVersion() {
		return version;
	}
}
