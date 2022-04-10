/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A {@link SourceFunction} that monitors a directory and sends events downstream when it detects
 * new files. Used together with {@link FileReadFunction}.
 *
 * @deprecated Internal class deprecated in favour of {@link ContinuousFileMonitoringFunction}.
 *
 * 每隔一定周期,监听目录下文件哪些新增了,去周期性的处理这些文件
 */
@Internal
@Deprecated
public class FileMonitoringFunction implements SourceFunction<Tuple3<String, Long, Long>> {
	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(FileMonitoringFunction.class);

	/**
	 * The watch type of the {@code FileMonitoringFunction}.
	 */
	public enum WatchType {
		ONLY_NEW_FILES, // Only new files will be processed.只处理新文件
		REPROCESS_WITH_APPENDED, // When some files are appended, all contents
									// of the files will be processed.如果文件发生修改,则修改后的所有内容都要重新加载
		PROCESS_ONLY_APPENDED // When some files are appended, only appended
								// contents will be processed. 只加载增量内容
	}

	private String path;
	private long interval;//监听的时间间隔
	private WatchType watchType;

	private Map<String, Long> offsetOfFiles;//key文件名,value文件已经处理的offset位置
	private Map<String, Long> modificationTimes;//key是文件名,value文件已知的最后保留时间

	private volatile boolean isRunning = true;

	public FileMonitoringFunction(String path, long interval, WatchType watchType) {
		this.path = path;
		this.interval = interval;
		this.watchType = watchType;
		this.modificationTimes = new HashMap<String, Long>();
		this.offsetOfFiles = new HashMap<String, Long>();
	}

	@Override
	public void run(SourceContext<Tuple3<String, Long, Long>> ctx) throws Exception {
		FileSystem fileSystem = FileSystem.get(new URI(path));

		while (isRunning) {
			List<String> files = listNewFiles(fileSystem);//返回待处理的文件
			for (String filePath : files) {
				if (watchType == WatchType.ONLY_NEW_FILES //文件是新文件
						|| watchType == WatchType.REPROCESS_WITH_APPENDED) {//加载文件全部内容
					ctx.collect(new Tuple3<String, Long, Long>(filePath, 0L, -1L));//文件名、开始位置是0,结束位置是-1
					offsetOfFiles.put(filePath, -1L);
				} else if (watchType == WatchType.PROCESS_ONLY_APPENDED) {//只加载增量内容
					long offset = 0;
					long fileSize = fileSystem.getFileStatus(new Path(filePath)).getLen();
					if (offsetOfFiles.containsKey(filePath)) {
						offset = offsetOfFiles.get(filePath);
					}

					ctx.collect(new Tuple3<String, Long, Long>(filePath, offset, fileSize));
					offsetOfFiles.put(filePath, fileSize);

					LOG.info("File processed: {}, {}, {}", filePath, offset, fileSize);
				}
			}

			Thread.sleep(interval);
		}
	}

	//获取path下满足条件的文件路径
	private List<String> listNewFiles(FileSystem fileSystem) throws IOException {
		List<String> files = new ArrayList<String>();

		FileStatus[] statuses = fileSystem.listStatus(new Path(path));//子目录集合

		if (statuses == null) {
			LOG.warn("Path does not exist: {}", path);
		} else {
			for (FileStatus status : statuses) {
				Path filePath = status.getPath();//文件
				String fileName = filePath.getName();//文件名
				long modificationTime = status.getModificationTime();//最后修改时间

				if (!isFiltered(fileName, modificationTime)) {
					files.add(filePath.toString());//要处理该文件
					modificationTimes.put(fileName, modificationTime);
				}
			}
		}

		return files;
	}

	/**
	 * 根据文件名 + 最后修改时间,觉得是否保留该文件
	 * @param fileName 文件名
	 * @param modificationTime 最后修改时间
	 * @return  false表示保留,true表示丢弃
	 */
	private boolean isFiltered(String fileName, long modificationTime) {

		if ((watchType == WatchType.ONLY_NEW_FILES && modificationTimes.containsKey(fileName)) //只保留新文件 -- 但文件已经存在过
				|| fileName.startsWith(".") || fileName.contains("_COPYING_")) { //隐藏文件、_COPYING_文件丢弃、
			return true;
		} else { //保留后期又修改的文件
			Long lastModification = modificationTimes.get(fileName);
			return lastModification != null && lastModification >= modificationTime;
		}
	}

	@Override
	public void cancel() {
		isRunning = false;
	}
}
