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

package org.apache.flink.runtime.rest.messages;

import org.apache.flink.util.Preconditions;

import java.util.Collection;

/**
 * This class defines the path/query {@link MessageParameter}s that can be used for a request.
 * 收纳一组请求参数集合
 * 如何将请求参数信息 组成 url
 */
public abstract class MessageParameters {

	/**
	 * Returns the collection of {@link MessagePathParameter} that the request supports. The collection should not be
	 * modifiable.
	 *
	 * @return collection of all supported message path parameters
	 * 一组必须存在的参数集合
	 */
	public abstract Collection<MessagePathParameter<?>> getPathParameters();

	/**
	 * Returns the collection of {@link MessageQueryParameter} that the request supports. The collection should not be
	 * modifiable.
	 *
	 * @return collection of all supported message query parameters
	 * 参数值是List<对象>的集合
	 */
	public abstract Collection<MessageQueryParameter<?>> getQueryParameters();

	/**
	 * Returns whether all mandatory parameters have been resolved.
	 *
	 * @return true, if all mandatory parameters have been resolved, false otherwise
	 * true表示所有必须存在的参数,都已经就位
	 */
	public final boolean isResolved() {
		return getPathParameters().stream().filter(MessageParameter::isMandatory).allMatch(MessageParameter::isResolved)
			&& getQueryParameters().stream().filter(MessageParameter::isMandatory).allMatch(MessageParameter::isResolved);
	}

	/**
	 * Resolves the given URL (e.g "jobs/:jobid") using the given path/query parameters.
	 *
	 * <p>This method will fail with an {@link IllegalStateException} if any mandatory parameter was not resolved.
	 *
	 * <p>Unresolved optional parameters will be ignored.
	 *
	 * @param genericUrl URL to resolve
	 * @param parameters message parameters parameters
	 * @return resolved url, e.g "/jobs/1234?state=running"
	 * @throws IllegalStateException if any mandatory parameter was not resolved
	 *
	 * 替换genericUrl中动态信息,比如jobs/:jobid中:jobid表示key,要替换成具体的value值
	 * 返回url
	 */
	public static String resolveUrl(String genericUrl, MessageParameters parameters) {
		//参数必须已经初始化完成
		Preconditions.checkState(parameters.isResolved(), "Not all mandatory message parameters were resolved.");

		StringBuilder path = new StringBuilder(genericUrl);
		for (MessageParameter<?> pathParameter : parameters.getPathParameters()) {//一组必须存在的参数集合
			if (pathParameter.isResolved()) {//已经存在value值
				int start = path.indexOf(':' + pathParameter.getKey());//发现key

				final String pathValue = Preconditions.checkNotNull(pathParameter.getValueAsString());//获取参数具体的值

				// only replace path parameters if they are present
				if (start != -1) {
					path.replace(start, start + pathParameter.getKey().length() + 1, pathValue);//替换具体的值
				}
			}
		}

		//设置查询语法,即?xx=value&xx=value1,value2,value3
		StringBuilder queryParameters = new StringBuilder();

		boolean isFirstQueryParameter = true;
		for (MessageQueryParameter<?> queryParameter : parameters.getQueryParameters()) {//获取List<T>类型参数值
			if (queryParameter.isResolved()) {//已经存在value值
				if (isFirstQueryParameter) {
					queryParameters.append('?');
					isFirstQueryParameter = false;
				} else {
					queryParameters.append('&');
				}
				queryParameters.append(queryParameter.getKey());
				queryParameters.append('=');
				queryParameters.append(queryParameter.getValueAsString());
			}
		}
		path.append(queryParameters);

		return path.toString();
	}
}
