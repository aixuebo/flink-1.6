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

package org.apache.flink.runtime.io.disk.iomanager;

import java.io.IOException;

/**
 * Callback to be executed on completion of an asynchronous I/O request.
 * <p>
 * Depending on success or failure, either {@link #requestSuccessful(Object)}
 * or {@link #requestSuccessful(Object)} is called.
 * 请求成功和失败时的动作--属于回调函数
 */
public interface RequestDoneCallback<T> {

	void requestSuccessful(T request);

	void requestFailed(T buffer, IOException e);
}
