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

package org.apache.flink.runtime.zookeeper.filesystem;

import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.RetrievableStateHandle;
import org.apache.flink.runtime.state.RetrievableStreamStateHandle;
import org.apache.flink.runtime.zookeeper.RetrievableStateStorageHelper;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.io.Serializable;

/**
 * {@link RetrievableStateStorageHelper} implementation which stores the state in the given filesystem path.
 *
 * @param <T> The type of the data that can be stored by this storage helper.
 *
 * 将T序列化后,存储到hdfs的一个文件中,返回可以可反序列化T的对象
 */
public class FileSystemStateStorageHelper<T extends Serializable> implements RetrievableStateStorageHelper<T> {

	private final Path rootPath;//hdfs的根目录

	private final String prefix;//文件前缀

	private final FileSystem fs;

	public FileSystemStateStorageHelper(String rootPath, String prefix) throws IOException {
		this(new Path(rootPath), prefix);
	}

	public FileSystemStateStorageHelper(Path rootPath, String prefix) throws IOException {
		this.rootPath = Preconditions.checkNotNull(rootPath, "Root path");
		this.prefix = Preconditions.checkNotNull(prefix, "Prefix");

		fs = FileSystem.get(rootPath.toUri());
	}

	//将T序列化后,存储到hdfs的一个文件中,返回可以可反序列化T的对象
	@Override
	public RetrievableStateHandle<T> store(T state) throws Exception {
		Exception latestException = null;

		for (int attempt = 0; attempt < 10; attempt++) {
			Path filePath = getNewFilePath();

			try (FSDataOutputStream outStream = fs.create(filePath, FileSystem.WriteMode.NO_OVERWRITE)) {
				InstantiationUtil.serializeObject(outStream, state);
				return new RetrievableStreamStateHandle<T>(filePath, outStream.getPos());
			}
			catch (Exception e) {
				latestException = e;
			}
		}

		throw new Exception("Could not open output stream for state backend", latestException);
	}

	//随机产生一个新的文件
	private Path getNewFilePath() {
		return new Path(rootPath, FileUtils.getRandomFilename(prefix));
	}
}
