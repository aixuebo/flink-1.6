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

package org.apache.flink.core.fs.local;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemFactory;

import java.net.URI;

/**
 * A factory for the {@link LocalFileSystem}.
 */
@PublicEvolving
public class LocalFileSystemFactory implements FileSystemFactory {

	//file
	@Override
	public String getScheme() {
		return LocalFileSystem.getLocalFsURI().getScheme();
	}

	//如何初始化文件系统
	@Override
	public void configure(Configuration config) {
		// the local file system takes no configuration, so nothing to do here
	}

	//创建一个文件系统--单例模式--因为文件系统创建是耗时的操作
	@Override
	public FileSystem create(URI fsUri) {
		return LocalFileSystem.getSharedInstance();
	}
}
