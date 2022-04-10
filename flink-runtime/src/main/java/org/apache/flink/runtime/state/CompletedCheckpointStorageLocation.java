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

package org.apache.flink.runtime.state;

import java.io.IOException;

/**
 * The CompletedCheckpointStorageLocation describes the storage aspect of a completed checkpoint.
 * It can be used to obtain access to the metadata, get a reference pointer to the checkpoint,
 * or to dispose the storage location.
 * 代表一个已经完成的checkpoint文件存储目录
 * 1.可以获取访问的元数据。
 * 2.销毁文件目录。
 * 3.checkpoint的路径描述
 */
public interface CompletedCheckpointStorageLocation extends java.io.Serializable {

	/**
	 * Gets the external pointer to the checkpoint. The pointer can be used to resume
	 * a program from the savepoint or checkpoint, and is typically passed as a command
	 * line argument, an HTTP request parameter, or stored in a system like ZooKeeper.
	 * checkpoint的路径描述,比如hdfs的全路径等信息
	 * 在hdfs存储上,设置的是path的全路径
	 */
	String getExternalPointer();

	/**
	 * Gets the state handle to the checkpoint's metadata.
	 * 元数据:如何打开文件流 、占用多少字节数等
	 */
	StreamStateHandle getMetadataHandle();

	/**
	 * Disposes the storage location. This method should be called after all state objects have
	 * been released. It typically disposes the base structure of the checkpoint storage,
	 * like the checkpoint directory.
	 * 销毁该checkpoint资源
	 */
	void disposeStorageLocation() throws IOException;
}
