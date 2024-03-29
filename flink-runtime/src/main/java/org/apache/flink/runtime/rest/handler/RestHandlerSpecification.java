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

package org.apache.flink.runtime.rest.handler;

import org.apache.flink.runtime.rest.HttpMethodWrapper;

/**
 * Rest handler interface which all rest handler implementation have to implement.
 * 描述请求往哪里发,以及什么方式发
 */
public interface RestHandlerSpecification {

	/**
	 * Returns the {@link HttpMethodWrapper} to be used for the request.
	 *
	 * @return http method to be used for the request
	 * 请求类型,get、post等
	 */
	HttpMethodWrapper getHttpMethod();

	/**
	 * Returns the generalized endpoint url that this request should be sent to, for example {@code /job/:jobid}.
	 *
	 * @return endpoint url that this request should be sent to
	 * 请求发送的目标url
	 */
	String getTargetRestEndpointURL();
}
