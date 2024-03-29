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

package org.apache.flink.runtime.rest.handler.cluster;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.messages.DashboardConfiguration;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import javax.annotation.Nonnull;

import java.time.ZonedDateTime;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Handler which returns the dashboard configuration.
 * 获取集群的信息，比如集群版本等信息
 *
 * 不需要输入,但response需要是DashboardConfiguration类型数据
 */
public class DashboardConfigHandler extends AbstractRestHandler<RestfulGateway, EmptyRequestBody, DashboardConfiguration, EmptyMessageParameters> {

	private final DashboardConfiguration dashboardConfiguration;

	public DashboardConfigHandler(
			CompletableFuture<String> localRestAddress,
			GatewayRetriever<? extends RestfulGateway> leaderRetriever,
			Time timeout,
			Map<String, String> responseHeaders,
			MessageHeaders<EmptyRequestBody, DashboardConfiguration, EmptyMessageParameters> messageHeaders,
			long refreshInterval) {
		super(localRestAddress, leaderRetriever, timeout, responseHeaders, messageHeaders);

		dashboardConfiguration = DashboardConfiguration.from(refreshInterval, ZonedDateTime.now());//构造信息
	}

	//直接返回对象
	@Override
	public CompletableFuture<DashboardConfiguration> handleRequest(@Nonnull HandlerRequest<EmptyRequestBody, EmptyMessageParameters> request, @Nonnull RestfulGateway gateway) {
		return CompletableFuture.completedFuture(dashboardConfiguration);
	}
}
