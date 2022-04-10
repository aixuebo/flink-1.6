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

package org.apache.flink.runtime.heartbeat;

import org.apache.flink.runtime.clusterframework.types.ResourceID;

import java.util.concurrent.CompletableFuture;

/**
 * Interface for the interaction with the {@link HeartbeatManager}. The heartbeat listener is used
 * for the following things:
 *
 * <ul>
 *     <li>Notifications about heartbeat timeouts</li>
 *     <li>Payload reports of incoming heartbeats</li>
 *     <li>Retrieval of payloads for outgoing heartbeats</li>
 * </ul>
 * @param <I> Type of the incoming payload 接受的信息类型
 * @param <O> Type of the outgoing payload 向外发送的信息类型
 */
public interface HeartbeatListener<I, O> {

	/**
	 * Callback which is called if a heartbeat for the machine identified by the given resource
	 * ID times out.
	 *
	 * @param resourceID Resource ID of the machine whose heartbeat has timed out
	 * 说明该job失去了心跳，产生回调,实现类实现该如何处理
	 */
	void notifyHeartbeatTimeout(ResourceID resourceID);

	/**
	 * Callback which is called whenever a heartbeat with an associated payload is received. The
	 * carried payload is given to this method.
	 *
	 * @param resourceID Resource ID identifying the sender of the payload 远程其他发送信息的节点id，即该节点向本服务器发送了信息
	 * @param payload Payload of the received heartbeat 带来的请求内容 I = input
	 * 当接收到某个资源带来的请求信息时，进行回调,实现类实现如何处理接收到的信息
	 *
	 * 即收到资源id发过来的内容I对象
	 */
	void reportPayload(ResourceID resourceID, I payload);

	/**
	 * Retrieves the payload value for the next heartbeat message. Since the operation can happen
	 * asynchronously, the result is returned wrapped in a future.
	 *
	 * @param resourceID Resource ID identifying the receiver of the payload
	 * @return Future containing the next payload for heartbeats
	 * 要向resourceID资源发送什么信息 --- 计算待response的内容
	 * 属于send主动发送行为,返回值是要发送出去的内容
	 */
	CompletableFuture<O> retrievePayload(ResourceID resourceID);
}
