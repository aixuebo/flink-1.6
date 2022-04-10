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

package org.apache.flink.queryablestate.network.messages;

import org.apache.flink.annotation.Internal;

/**
 * Expected message types during the communication between
 * {@link org.apache.flink.queryablestate.network.Client client} and
 * {@link org.apache.flink.queryablestate.network.AbstractServerBase server}.
 */
@Internal
public enum MessageType {

	/** The message is a request. 发送一个请求*/
	REQUEST,

	/** The message is a successful response.请求成功,并且有返回值response */
	REQUEST_RESULT,

	/** The message indicates a protocol-related failure.请求失败,并且带回来失败原因 */
	REQUEST_FAILURE,

	/** The message indicates a server failure.请求的服务器有问题,服务后期不能再提供服务 */
	SERVER_FAILURE
}
