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

/**
 * Interface for components which can be sent heartbeats and from which one can request a
 * heartbeat response. Both the heartbeat response as well as the heartbeat request can carry a
 * payload. This payload is reported to the heartbeat target and contains additional information.
 * The payload can be empty which is indicated by a null value.
 *
 * @param <I> Type of the payload which is sent to the heartbeat target 心跳交互的内容对象
 *
 * 向一个资源 接收/发送 心跳，心跳内容是I
 *
 * 相当于服务端的socket对象，即持有该对象的时候已经知道客户端是谁了，即每一个客户端都对应一个该对象
 *
 *
 * 逆向回复信息,属于response
 */
public interface HeartbeatTarget<I> {

	/**
	 * Sends a heartbeat response to the target. Each heartbeat response can carry a payload which
	 * contains additional information for the heartbeat target.
	 *
	 * @param heartbeatOrigin Resource ID identifying the machine for which a heartbeat shall be reported.
	 * @param heartbeatPayload Payload of the heartbeat. Null indicates an empty payload.
	 * 接受来自ResourceID(比如jobmanager的id)的心跳，I表示心跳内容
	 *
	 * 属于接受消息
	 *
	 * 比如task节点，持有jobManager的心跳,平时接收jobManager的心跳是正常逻辑，但如果需要向jobManager发送消息,则需要持有该类.此时ResourceID表示的是task节点的uuid
	 */
	void receiveHeartbeat(ResourceID heartbeatOrigin, I heartbeatPayload);

	/**
	 * Requests a heartbeat from the target. Each heartbeat request can carry a payload which
	 * contains additional information for the heartbeat target.
	 *
	 * @param requestOrigin Resource ID identifying the machine issuing the heartbeat request.谁发送的请求,即主动发请求的是谁
	 * @param heartbeatPayload Payload of the heartbeat request. Null indicates an empty payload. 发送请求的内容
	 *
	 * 发送给客户端信息，参数标注好自己的id、以及自己要发送给客户端的信息内容
	 * 因为已经知道客户端是谁了，所以只需要告诉客户端自己是谁。
	 *
	 * 即客户端收到信息后，需要去向哪个资源发送心跳信息
	 *
	 * 属于发送消息
	 */
	void requestHeartbeat(ResourceID requestOrigin, I heartbeatPayload);
}
