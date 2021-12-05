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

package org.apache.flink.runtime.blob;

import java.io.Closeable;

/**
 * Service interface for the BlobStore which allows to close and clean up its data.
 * 根据配置文件,获取存储数据的path，创建一个服务，可以向该path写入数据
 * 因此任意一个节点都可以用来当服务节点,服务创建需要的资源很小
 */
public interface BlobStoreService extends BlobStore, Closeable {

	/**
	 * Closes and cleans up the store. This entails the deletion of all blobs.
	 * 删除全部job的全部文件数据，即清空BlobStoreService服务维护的所有数据
	 */
	void closeAndCleanupAllData();
}
