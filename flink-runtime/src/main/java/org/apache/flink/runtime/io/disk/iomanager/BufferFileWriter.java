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

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.util.event.NotificationListener;

import java.io.IOException;

//以buffer方式写入数据
public interface BufferFileWriter extends BlockChannelWriterWithCallback<Buffer> {

	/**
	 * Returns the number of outstanding requests.
	 * 当前有多少个写请求尚未完成
	 */
	int getNumberOfOutstandingRequests();

	/**
	 * Registers a listener, which is notified after all outstanding requests have been processed.
	 * 注册一个listener,所有的任务都执行完后,接收通知
	 */
	boolean registerAllRequestsProcessedListener(NotificationListener listener) throws IOException;

}
