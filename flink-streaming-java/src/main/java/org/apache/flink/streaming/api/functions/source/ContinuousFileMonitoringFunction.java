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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.io.FilePathFilter;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * This is the single (non-parallel) monitoring task which takes a {@link FileInputFormat}
 * and, depending on the {@link FileProcessingMode} and the {@link FilePathFilter}, it is responsible for:
 *
 * <ol>
 *     <li>Monitoring a user-provided path.</li>
 *     <li>Deciding which files should be further read and processed.</li>
 *     <li>Creating the {@link FileInputSplit splits} corresponding to those files.</li>
 *     <li>Assigning them to downstream tasks for further processing.</li>
 * </ol>
 *
 * 非并行任务。
 * 1.监控给定路径下的文件。
 * 2.决定应该进一步读取/操作的是哪些文件。
 * 3.对待读取的信息创建FileInputSplit
 * 4.分发出去,让下游去读取/处理这些FileInputSplit
 *
 * <p>The splits to be read are forwarded to the downstream {@link ContinuousFileReaderOperator}
 * which can have parallelism greater than one.
 * 下游ContinuousFileReaderOperator是可以并行的去读取/操作文件的
 *
 * <p><b>IMPORTANT NOTE: </b> Splits are forwarded downstream for reading in ascending modification time order,
 * based on the modification time of the files they belong to.
 *
 * 监控一个目录，如果目录下文件被修改了，则返回哪些文件被修改了，发送给下游哪些文件被修改了，让下游去重新加载数据。
 */
@Internal
public class ContinuousFileMonitoringFunction<OUT>
	extends RichSourceFunction<TimestampedFileInputSplit> implements CheckpointedFunction {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(ContinuousFileMonitoringFunction.class);

	/**
	 * The minimum interval allowed between consecutive path scans.
	 *
	 * <p><b>NOTE:</b> Only applicable to the {@code PROCESS_CONTINUOUSLY} mode.
	 */
	public static final long MIN_MONITORING_INTERVAL = 1L;

	/** The path to monitor. */
	private final String path;//监听目录

	/** The parallelism of the downstream readers. */
	private final int readerParallelism;//下游读取的并发度

	/** The {@link FileInputFormat} to be read. */
	private final FileInputFormat<OUT> format;

	/** The interval between consecutive path scans. 扫描文件的时间间隔*/
	private final long interval;

	/** Which new data to process (see {@link FileProcessingMode}. */
	private final FileProcessingMode watchType;//监听文件的模式

	/** The maximum file modification time seen so far. */
	private volatile long globalModificationTime = Long.MIN_VALUE;

	private transient Object checkpointLock;

	private volatile boolean isRunning = true;

	private transient ListState<Long> checkpointedState;

	public ContinuousFileMonitoringFunction(
		FileInputFormat<OUT> format,
		FileProcessingMode watchType,//监控模式
		int readerParallelism,//下游读取的并发度
		long interval) {//扫描文件的时间间隔

		//interval不允许比最小值小,即interval要>最小值
		Preconditions.checkArgument(
			watchType == FileProcessingMode.PROCESS_ONCE || interval >= MIN_MONITORING_INTERVAL,
			"The specified monitoring interval (" + interval + " ms) is smaller than the minimum " +
				"allowed one (" + MIN_MONITORING_INTERVAL + " ms)."
		);

		Preconditions.checkArgument(
			format.getFilePaths().length == 1,
			"FileInputFormats with multiple paths are not supported yet.");

		this.format = Preconditions.checkNotNull(format, "Unspecified File Input Format.");
		this.path = Preconditions.checkNotNull(format.getFilePaths()[0].toString(), "Unspecified Path.");

		this.interval = interval;
		this.watchType = watchType;
		this.readerParallelism = Math.max(readerParallelism, 1);
		this.globalModificationTime = Long.MIN_VALUE;
	}

	@VisibleForTesting
	public long getGlobalModificationTime() {
		return this.globalModificationTime;
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {

		Preconditions.checkState(this.checkpointedState == null,
			"The " + getClass().getSimpleName() + " has already been initialized.");

		this.checkpointedState = context.getOperatorStateStore().getListState(
			new ListStateDescriptor<>(
				"file-monitoring-state",
				LongSerializer.INSTANCE
			)
		);

		if (context.isRestored()) {
			LOG.info("Restoring state for the {}.", getClass().getSimpleName());

			List<Long> retrievedStates = new ArrayList<>();
			for (Long entry : this.checkpointedState.get()) {
				retrievedStates.add(entry);
			}

			// given that the parallelism of the function is 1, we can only have 1 or 0 retrieved items.
			// the 0 is for the case that we are migrating from a previous Flink version.

			Preconditions.checkArgument(retrievedStates.size() <= 1,
				getClass().getSimpleName() + " retrieved invalid state.");

			if (retrievedStates.size() == 1 && globalModificationTime != Long.MIN_VALUE) {
				// this is the case where we have both legacy and new state.
				// The two should be mutually exclusive for the operator, thus we throw the exception.

				throw new IllegalArgumentException(
					"The " + getClass().getSimpleName() + " has already restored from a previous Flink version.");

			} else if (retrievedStates.size() == 1) {
				this.globalModificationTime = retrievedStates.get(0);
				if (LOG.isDebugEnabled()) {
					LOG.debug("{} retrieved a global mod time of {}.",
						getClass().getSimpleName(), globalModificationTime);
				}
			}

		} else {
			LOG.info("No state to restore for the {}.", getClass().getSimpleName());
		}
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		format.configure(parameters);

		if (LOG.isDebugEnabled()) {
			LOG.debug("Opened {} (taskIdx= {}) for path: {}",
				getClass().getSimpleName(), getRuntimeContext().getIndexOfThisSubtask(), path);
		}
	}

	@Override
	public void run(SourceFunction.SourceContext<TimestampedFileInputSplit> context) throws Exception {
		//读取文件路径
		Path p = new Path(path);
		FileSystem fileSystem = FileSystem.get(p.toUri());
		if (!fileSystem.exists(p)) {
			throw new FileNotFoundException("The provided file path " + path + " does not exist.");
		}

		checkpointLock = context.getCheckpointLock();
		switch (watchType) {
			case PROCESS_CONTINUOUSLY:
				while (isRunning) {
					synchronized (checkpointLock) {
						monitorDirAndForwardSplits(fileSystem, context);
					}
					Thread.sleep(interval);
				}

				// here we do not need to set the running to false and the
				// globalModificationTime to Long.MAX_VALUE because to arrive here,
				// either close() or cancel() have already been called, so this
				// is already done.

				break;
			case PROCESS_ONCE:
				synchronized (checkpointLock) {

					// the following check guarantees that if we restart
					// after a failure and we managed to have a successful
					// checkpoint, we will not reprocess the directory.

					if (globalModificationTime == Long.MIN_VALUE) {//保险起见,设置最后修改时间为最小值,确保文件可以被处理
						monitorDirAndForwardSplits(fileSystem, context);
						globalModificationTime = Long.MAX_VALUE;//设置最后修改时间为最大值,因为不需要再处理未来追加的文件信息了
					}
					isRunning = false;
				}
				break;
			default:
				isRunning = false;
				throw new RuntimeException("Unknown WatchType" + watchType);
		}
	}

	//真实处理文件
	private void monitorDirAndForwardSplits(FileSystem fs,
											SourceContext<TimestampedFileInputSplit> context) throws IOException {
		assert (Thread.holdsLock(checkpointLock));

		Map<Path, FileStatus> eligibleFiles = listEligibleFiles(fs, new Path(path));//返回已经发生修改的<文件路径,文件状态>

		//key最后修改时间 value同时间的数据块集合
		Map<Long, List<TimestampedFileInputSplit>> splitsSortedByModTime = getInputSplitsSortedByModTime(eligibleFiles);//对修改过的文件,进行拆分成数据块,并且数据块按照时间顺序排序

		for (Map.Entry<Long, List<TimestampedFileInputSplit>> splits: splitsSortedByModTime.entrySet()) {
			long modificationTime = splits.getKey();
			for (TimestampedFileInputSplit split: splits.getValue()) {
				LOG.info("Forwarding split: " + split);
				context.collect(split);//发送每一个数据块
			}
			// update the global modification time
			globalModificationTime = Math.max(globalModificationTime, modificationTime);//修改最后时间戳更新到哪里了
		}
	}

	/**
	 * Creates the input splits to be forwarded to the downstream tasks of the
	 * {@link ContinuousFileReaderOperator}. Splits are sorted <b>by modification time</b> before
	 * being forwarded and only splits belonging to files in the {@code eligibleFiles}
	 * list will be processed.
	 * @param eligibleFiles The files to process. 等待处理的文件<文件路径,文件状态>
	 * 返回排序后的结果集
	 */
	private Map<Long, List<TimestampedFileInputSplit>> getInputSplitsSortedByModTime(
				Map<Path, FileStatus> eligibleFiles) throws IOException {

		Map<Long, List<TimestampedFileInputSplit>> splitsByModTime = new TreeMap<>();
		if (eligibleFiles.isEmpty()) {
			return splitsByModTime;
		}

		for (FileInputSplit split: format.createInputSplits(readerParallelism)) {//拆分成若干个文件块
			FileStatus fileStatus = eligibleFiles.get(split.getPath());//该数据块是否在待处理的文件中
			if (fileStatus != null) {
				Long modTime = fileStatus.getModificationTime();//文件被处理的最后时间戳
				List<TimestampedFileInputSplit> splitsToForward = splitsByModTime.get(modTime);
				if (splitsToForward == null) {
					splitsToForward = new ArrayList<>();
					splitsByModTime.put(modTime, splitsToForward);
				}
				splitsToForward.add(new TimestampedFileInputSplit(
					modTime, split.getSplitNumber(), split.getPath(),
					split.getStart(), split.getLength(), split.getHostnames()));
			}
		}
		return splitsByModTime;
	}

	/**
	 * Returns the paths of the files not yet processed.
	 * @param fileSystem The filesystem where the monitored directory resides.
	 * 返回已经发生修改的<文件路径,文件状态>
	 */
	private Map<Path, FileStatus> listEligibleFiles(FileSystem fileSystem, Path path) throws IOException {

		final FileStatus[] statuses;
		try {
			statuses = fileSystem.listStatus(path);
		} catch (IOException e) {
			// we may run into an IOException if files are moved while listing their status
			// delay the check for eligible files in this case
			return Collections.emptyMap();
		}

		if (statuses == null) {
			LOG.warn("Path does not exist: {}", path);
			return Collections.emptyMap();
		} else {
			Map<Path, FileStatus> files = new HashMap<>();
			// handle the new files
			for (FileStatus status : statuses) {
				if (!status.isDir()) {
					Path filePath = status.getPath();
					long modificationTime = status.getModificationTime();
					if (!shouldIgnore(filePath, modificationTime)) {//不应该忽略,说明有新修改的文件了
						files.put(filePath, status);
					}
				} else if (format.getNestedFileEnumeration() && format.acceptFile(status)){
					files.putAll(listEligibleFiles(fileSystem, status.getPath()));
				}
			}
			return files;
		}
	}

	/**
	 * Returns {@code true} if the file is NOT to be processed further.
	 * This happens if the modification time of the file is smaller than
	 * the {@link #globalModificationTime}.
	 * @param filePath the path of the file to check.
	 * @param modificationTime the modification time of the file.
	 * true表示应该忽略
	 */
	private boolean shouldIgnore(Path filePath, long modificationTime) {
		assert (Thread.holdsLock(checkpointLock));
		boolean shouldIgnore = modificationTime <= globalModificationTime;
		if (shouldIgnore && LOG.isDebugEnabled()) {
			LOG.debug("Ignoring " + filePath + ", with mod time= " + modificationTime +
				" and global mod time= " + globalModificationTime);
		}
		return shouldIgnore;
	}

	@Override
	public void close() throws Exception {
		super.close();

		if (checkpointLock != null) {
			synchronized (checkpointLock) {
				globalModificationTime = Long.MAX_VALUE;
				isRunning = false;
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("Closed File Monitoring Source for path: " + path + ".");
		}
	}

	@Override
	public void cancel() {
		if (checkpointLock != null) {
			// this is to cover the case where cancel() is called before the run()
			synchronized (checkpointLock) {
				globalModificationTime = Long.MAX_VALUE;
				isRunning = false;
			}
		} else {
			globalModificationTime = Long.MAX_VALUE;
			isRunning = false;
		}
	}

	//	---------------------			Checkpointing			--------------------------

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		Preconditions.checkState(this.checkpointedState != null,
			"The " + getClass().getSimpleName() + " state has not been properly initialized.");

		this.checkpointedState.clear();
		this.checkpointedState.add(this.globalModificationTime);

		if (LOG.isDebugEnabled()) {
			LOG.debug("{} checkpointed {}.", getClass().getSimpleName(), globalModificationTime);
		}
	}
}
