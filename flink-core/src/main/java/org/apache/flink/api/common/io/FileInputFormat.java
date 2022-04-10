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

package org.apache.flink.api.common.io;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.io.compression.Bzip2InputStreamFactory;
import org.apache.flink.api.common.io.compression.DeflateInflaterInputStreamFactory;
import org.apache.flink.api.common.io.compression.GzipInflaterInputStreamFactory;
import org.apache.flink.api.common.io.compression.InflaterInputStreamFactory;
import org.apache.flink.api.common.io.compression.XZInputStreamFactory;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.fs.BlockLocation;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The base class for {@link RichInputFormat}s that read from files. For specific input types the
 * {@link #nextRecord(Object)} and {@link #reachedEnd()} methods need to be implemented.
 * Additionally, one may override {@link #open(FileInputSplit)} and {@link #close()} to
 * change the life cycle behavior.
 * 
 * <p>After the {@link #open(FileInputSplit)} method completed, the file input data is available
 * from the {@link #stream} field.</p>
 */
@Public
public abstract class FileInputFormat<OT> extends RichInputFormat<OT, FileInputSplit> {
	
	// -------------------------------------- Constants -------------------------------------------
	
	private static final Logger LOG = LoggerFactory.getLogger(FileInputFormat.class);
	
	private static final long serialVersionUID = 1L;
	
	
	/**
	 * The fraction that the last split may be larger than the others.
	 */
	private static final float MAX_SPLIT_SIZE_DISCREPANCY = 1.1f;

	/**
	 * The timeout (in milliseconds) to wait for a filesystem stream to respond.
	 * 读取文件系统流的超时时间
	 */
	private static long DEFAULT_OPENING_TIMEOUT;

	/**
	 * A mapping of file extensions to decompression algorithms based on DEFLATE. Such compressions lead to
	 * unsplittable files.
	 * 文件压缩方式  key是压缩方式name,value具体的压缩方式
	 */
	protected static final Map<String, InflaterInputStreamFactory<?>> INFLATER_INPUT_STREAM_FACTORIES =
			new HashMap<String, InflaterInputStreamFactory<?>>();
	
	/**
	 * The splitLength is set to -1L for reading the whole split.
	 * 如果是-1,表示读取整个split文件块
	 */
	protected static final long READ_WHOLE_SPLIT_FLAG = -1L;

	static {
		initDefaultsFromConfiguration(GlobalConfiguration.loadConfiguration());
		initDefaultInflaterInputStreamFactories();
	}

	/**
	 * Initialize defaults for input format. Needs to be a static method because it is configured for local
	 * cluster execution, see LocalFlinkMiniCluster.
	 * @param configuration The configuration to load defaults from
	 * 初始化读取文件系统的超时时间
	 */
	private static void initDefaultsFromConfiguration(Configuration configuration) {
		final long to = configuration.getLong(ConfigConstants.FS_STREAM_OPENING_TIMEOUT_KEY,
			ConfigConstants.DEFAULT_FS_STREAM_OPENING_TIMEOUT);//读取文件系统流的超时时间
		if (to < 0) {
			LOG.error("Invalid timeout value for filesystem stream opening: " + to + ". Using default value of " +
				ConfigConstants.DEFAULT_FS_STREAM_OPENING_TIMEOUT);
			DEFAULT_OPENING_TIMEOUT = ConfigConstants.DEFAULT_FS_STREAM_OPENING_TIMEOUT;
		} else if (to == 0) {
			DEFAULT_OPENING_TIMEOUT = 300000; // 5 minutes
		} else {
			DEFAULT_OPENING_TIMEOUT = to;
		}
	}

	private static void initDefaultInflaterInputStreamFactories() {
		InflaterInputStreamFactory<?>[] defaultFactories = {
				DeflateInflaterInputStreamFactory.getInstance(),
				GzipInflaterInputStreamFactory.getInstance(),
				Bzip2InputStreamFactory.getInstance(),
				XZInputStreamFactory.getInstance(),
		};
		for (InflaterInputStreamFactory<?> inputStreamFactory : defaultFactories) {
			for (String fileExtension : inputStreamFactory.getCommonFileExtensions()) {
				registerInflaterInputStreamFactory(fileExtension, inputStreamFactory);
			}
		}
	}

	/**
	 * Registers a decompression algorithm through a {@link org.apache.flink.api.common.io.compression.InflaterInputStreamFactory}
	 * with a file extension for transparent decompression.
	 * @param fileExtension of the compressed files
	 * @param factory to create an {@link java.util.zip.InflaterInputStream} that handles the decompression format
	 * 注册压缩方式
	 */
	public static void registerInflaterInputStreamFactory(String fileExtension, InflaterInputStreamFactory<?> factory) {
		synchronized (INFLATER_INPUT_STREAM_FACTORIES) {
			if (INFLATER_INPUT_STREAM_FACTORIES.put(fileExtension, factory) != null) {
				LOG.warn("Overwriting an existing decompression algorithm for \"{}\" files.", fileExtension);
			}
		}
	}

	//获取压缩方式
	protected static InflaterInputStreamFactory<?> getInflaterInputStreamFactory(String fileExtension) {
		synchronized (INFLATER_INPUT_STREAM_FACTORIES) {
			return INFLATER_INPUT_STREAM_FACTORIES.get(fileExtension);
		}
	}

	/**
	 * Returns the extension of a file name (!= a path).
	 * @return the extension of the file name or {@code null} if there is no extension.
	 * 获取文件的后缀名
	 */
	protected static String extractFileExtension(String fileName) {
		checkNotNull(fileName);
		int lastPeriodIndex = fileName.lastIndexOf('.');
		if (lastPeriodIndex < 0){
			return null;
		} else {
			return fileName.substring(lastPeriodIndex + 1);
		}
	}
	
	// --------------------------------------------------------------------------------------------
	//  Variables for internal operation.
	//  They are all transient, because we do not want them so be serialized 
	// --------------------------------------------------------------------------------------------
	
	/**
	 * The input stream reading from the input file.
	 * 如何读取一个文件
	 */
	protected transient FSDataInputStream stream;

	/**
	 * The start of the split that this parallel instance must consume.
	 * 当前处理的数据块,读取的开始位置
	 */
	protected transient long splitStart;

	/**
	 * The length of the split that this parallel instance must consume.
	 * 当前处理的数据块,要读取的长度
	 */
	protected transient long splitLength;

	/**
	 * The current split that this parallel instance must consume.
	 * 当前正在处理的数据块
	 */
	protected transient FileInputSplit currentSplit;
	
	// --------------------------------------------------------------------------------------------
	//  The configuration parameters. Configured on the instance and serialized to be shipped.
	// --------------------------------------------------------------------------------------------
	
	/**
	 * The path to the file that contains the input.
	 *
	 * @deprecated Please override {@link FileInputFormat#supportsMultiPaths()} and
	 *             use {@link FileInputFormat#getFilePaths()} and {@link FileInputFormat#setFilePaths(Path...)}.
	 * 当读取的文件只有一个的时候,文件路径
	 */
	@Deprecated
	protected Path filePath;

	/**
	 * The list of paths to files and directories that contain the input.
	 * 要读取的多个文件路径
	 */
	private Path[] filePaths;
	
	/**
	 * The minimal split size, set by the configure() method.
	 * 拆分的最小块大小
	 */
	protected long minSplitSize = 0; 
	
	/**
	 * The desired number of splits, as set by the configure() method.
	 * 建议拆分多少个数据块
	 */
	protected int numSplits = -1;
	
	/**
	 * Stream opening timeout.
	 * 读取文件系统流的超时时间
	 */
	protected long openTimeout = DEFAULT_OPENING_TIMEOUT;
	
	/**
	 * Some file input formats are not splittable on a block level (avro, deflate)
	 * Therefore, the FileInputFormat can only read whole files.
	 * true表示文件不能拆分,属于压缩文件
	 */
	protected boolean unsplittable = false;

	/**
	 * The flag to specify whether recursive traversal of the input directory
	 * structure is enabled.
	 * true表示遇到目录是否递归进行读取文件
	 */
	protected boolean enumerateNestedFiles = false;

	/**
	 * Files filter for determining what files/directories should be included.
	 */
	private FilePathFilter filesFilter = new GlobFilePathFilter();

	// --------------------------------------------------------------------------------------------
	//  Constructors
	// --------------------------------------------------------------------------------------------	

	public FileInputFormat() {}

	protected FileInputFormat(Path filePath) {
		if (filePath != null) {
			setFilePath(filePath);
		}
	}
	
	// --------------------------------------------------------------------------------------------
	//  Getters/setters for the configurable parameters
	// --------------------------------------------------------------------------------------------
	
	/**
	 *
	 * @return The path of the file to read.
	 *
	 * @deprecated Please use getFilePaths() instead.
	 */
	@Deprecated
	public Path getFilePath() {

		if (supportsMultiPaths()) {
			if (this.filePaths == null || this.filePaths.length == 0) {
				return null;
			} else if (this.filePaths.length == 1) {
				return this.filePaths[0];
			} else {
				throw new UnsupportedOperationException(
					"FileInputFormat is configured with multiple paths. Use getFilePaths() instead.");
			}
		} else {
			return filePath;
		}
	}
	
	/**
	 * Returns the paths of all files to be read by the FileInputFormat.
	 * 
	 * @return The list of all paths to read.
	 */
	public Path[] getFilePaths() {

		if (supportsMultiPaths()) {
			if (this.filePaths == null) {
				return new Path[0];
			}
			return this.filePaths;
		} else {
			if (this.filePath == null) {
				return new Path[0];
			}
			return new Path[] {filePath};
		}
	}
	
	public void setFilePath(String filePath) {
		if (filePath == null) {
			throw new IllegalArgumentException("File path cannot be null.");
		}

		// TODO The job-submission web interface passes empty args (and thus empty
		// paths) to compute the preview graph. The following is a workaround for
		// this situation and we should fix this.

		// comment (Stephan Ewen) this should be no longer relevant with the current Java/Scala APIs.
		if (filePath.isEmpty()) {
			setFilePath(new Path());
			return;
		}

		try {
			this.setFilePath(new Path(filePath));
		} catch (RuntimeException rex) {
			throw new RuntimeException("Could not create a valid URI from the given file path name: " + rex.getMessage());
		}
	}
	
	/**
	 * Sets a single path of a file to be read.
	 *
	 * @param filePath The path of the file to read.
	 */
	public void setFilePath(Path filePath) {
		if (filePath == null) {
			throw new IllegalArgumentException("File path must not be null.");
		}

		setFilePaths(filePath);
	}
	
	/**
	 * Sets multiple paths of files to be read.
	 * 
	 * @param filePaths The paths of the files to read.
	 */
	public void setFilePaths(String... filePaths) {
		Path[] paths = new Path[filePaths.length];
		for (int i = 0; i < paths.length; i++) {
			paths[i] = new Path(filePaths[i]);
		}
		setFilePaths(paths);
	}

	/**
	 * Sets multiple paths of files to be read.
	 *
	 * @param filePaths The paths of the files to read.
	 */
	public void setFilePaths(Path... filePaths) {
		if (!supportsMultiPaths() && filePaths.length > 1) {
			throw new UnsupportedOperationException(
				"Multiple paths are not supported by this FileInputFormat.");
		}
		if (filePaths.length < 1) {
			throw new IllegalArgumentException("At least one file path must be specified.");
		}
		if (filePaths.length == 1) {
			// set for backwards compatibility
			this.filePath = filePaths[0];
		} else {
			// clear file path in case it had been set before
			this.filePath = null;
		}

		this.filePaths = filePaths;
	}
	
	public long getMinSplitSize() {
		return minSplitSize;
	}
	
	public void setMinSplitSize(long minSplitSize) {
		if (minSplitSize < 0) {
			throw new IllegalArgumentException("The minimum split size cannot be negative.");
		}

		this.minSplitSize = minSplitSize;
	}
	
	public int getNumSplits() {
		return numSplits;
	}
	
	public void setNumSplits(int numSplits) {
		if (numSplits < -1 || numSplits == 0) {
			throw new IllegalArgumentException("The desired number of splits must be positive or -1 (= don't care).");
		}
		
		this.numSplits = numSplits;
	}
	
	public long getOpenTimeout() {
		return openTimeout;
	}
	
	public void setOpenTimeout(long openTimeout) {
		if (openTimeout < 0) {
			throw new IllegalArgumentException("The timeout for opening the input splits must be positive or zero (= infinite).");
		}
		this.openTimeout = openTimeout;
	}

	public void setNestedFileEnumeration(boolean enable) {
		this.enumerateNestedFiles = enable;
	}

	public boolean getNestedFileEnumeration() {
		return this.enumerateNestedFiles;
	}

	// --------------------------------------------------------------------------------------------
	// Getting information about the split that is currently open
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Gets the start of the current split.
	 *
	 * @return The start of the split.
	 */
	public long getSplitStart() {
		return splitStart;
	}
	
	/**
	 * Gets the length or remaining length of the current split.
	 *
	 * @return The length or remaining length of the current split.
	 */
	public long getSplitLength() {
		return splitLength;
	}

	public void setFilesFilter(FilePathFilter filesFilter) {
		this.filesFilter = Preconditions.checkNotNull(filesFilter, "Files filter should not be null");
	}

	// --------------------------------------------------------------------------------------------
	//  Pre-flight: Configuration, Splits, Sampling
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Configures the file input format by reading the file path from the configuration.
	 * 
	 * @see org.apache.flink.api.common.io.InputFormat#configure(org.apache.flink.configuration.Configuration)
	 */
	@Override
	public void configure(Configuration parameters) {

		if (getFilePaths().length == 0) {
			// file path was not specified yet. Try to set it from the parameters.
			String filePath = parameters.getString(FILE_PARAMETER_KEY, null);
			if (filePath == null) {
				throw new IllegalArgumentException("File path was not specified in input format or configuration.");
			} else {
				setFilePath(filePath);
			}
		}

		if (!this.enumerateNestedFiles) {
			this.enumerateNestedFiles = parameters.getBoolean(ENUMERATE_NESTED_FILES_FLAG, false);
		}
	}

	/**
	 * Obtains basic file statistics containing only file size. If the input is a directory, then the size is the sum of all contained files.
	 * 
	 * @see org.apache.flink.api.common.io.InputFormat#getStatistics(org.apache.flink.api.common.io.statistics.BaseStatistics)
	 * 统计整个数据源的字节数量、最后更新时间
	 */
	@Override
	public FileBaseStatistics getStatistics(BaseStatistics cachedStats) throws IOException {
		
		final FileBaseStatistics cachedFileStats = cachedStats instanceof FileBaseStatistics ?
			(FileBaseStatistics) cachedStats : null;
				
		try {
			return getFileStats(cachedFileStats, getFilePaths(), new ArrayList<>(getFilePaths().length));
		} catch (IOException ioex) {
			if (LOG.isWarnEnabled()) {
				LOG.warn("Could not determine statistics for paths '" + Arrays.toString(getFilePaths()) + "' due to an io error: "
						+ ioex.getMessage());
			}
		}
		catch (Throwable t) {
			if (LOG.isErrorEnabled()) {
				LOG.error("Unexpected problem while getting the file statistics for paths '" + Arrays.toString(getFilePaths()) + "': "
						+ t.getMessage(), t);
			}
		}
		
		// no statistics available
		return null;
	}

	/**
	 *
	 * @param cachedStats 需要填充的统计对象
	 * @param filePaths 需要被处理的所有文件集合
	 * @param files 每一个文件对应一个文件对象
	 * @return
	 * @throws IOException
	 */
	protected FileBaseStatistics getFileStats(FileBaseStatistics cachedStats, Path[] filePaths, ArrayList<FileStatus> files) throws IOException {

		long totalLength = 0;
		long latestModTime = 0;

		for (Path path : filePaths) {//循环要处理的所有文件
			final FileSystem fs = FileSystem.get(path.toUri());
			final FileBaseStatistics stats = getFileStats(cachedStats, path, fs, files);

			if (stats.getTotalInputSize() == BaseStatistics.SIZE_UNKNOWN) {
				totalLength = BaseStatistics.SIZE_UNKNOWN;
			} else if (totalLength != BaseStatistics.SIZE_UNKNOWN) {
				totalLength += stats.getTotalInputSize();
			}
			latestModTime = Math.max(latestModTime, stats.getLastModificationTime());
		}

		// check whether the cached statistics are still valid, if we have any
		if (cachedStats != null && latestModTime <= cachedStats.getLastModificationTime()) {
			return cachedStats;
		}

		return new FileBaseStatistics(latestModTime, totalLength, BaseStatistics.AVG_RECORD_BYTES_UNKNOWN);
	}

	/**
	 * 1.将filePath路径下文件都添加到files
	 * 2.计算截止到当前,所有文件的最新更新时间
	 */
	protected FileBaseStatistics getFileStats(FileBaseStatistics cachedStats, Path filePath, FileSystem fs, ArrayList<FileStatus> files) throws IOException {

		// get the file info and check whether the cached statistics are still valid.
		final FileStatus file = fs.getFileStatus(filePath);
		long totalLength = 0;

		// enumerate all files
		if (file.isDir()) {
			totalLength += addFilesInDir(file.getPath(), files, false);
		} else {
			files.add(file);
			testForUnsplittable(file);
			totalLength += file.getLen();
		}

		// check the modification time stamp
		long latestModTime = 0;
		for (FileStatus f : files) {
			latestModTime = Math.max(f.getModificationTime(), latestModTime);
		}

		// check whether the cached statistics are still valid, if we have any
		if (cachedStats != null && latestModTime <= cachedStats.getLastModificationTime()) {
			return cachedStats;
		}

		// sanity check
		if (totalLength <= 0) {
			totalLength = BaseStatistics.SIZE_UNKNOWN;
		}
		return new FileBaseStatistics(latestModTime, totalLength, BaseStatistics.AVG_RECORD_BYTES_UNKNOWN);
	}

	@Override
	public LocatableInputSplitAssigner getInputSplitAssigner(FileInputSplit[] splits) {
		return new LocatableInputSplitAssigner(splits);
	}

	/**
	 * Computes the input splits for the file. By default, one file block is one split. If more splits
	 * are requested than blocks are available, then a split may be a fraction of a block and splits may cross
	 * block boundaries.
	 * 
	 * @param minNumSplits The minimum desired number of file splits.
	 * @return The computed file splits.
	 * 
	 * @see org.apache.flink.api.common.io.InputFormat#createInputSplits(int)
	 * 文件拆分数据块
	 *
	1.设置一个建议最小数a、设置一个分块数b --- 理论上都是你设置的，肯定a<b 所以结果是b,即以你设置的分快数为准。如果没设置,则使用参数的最小分快数a
	minNumSplits = Math.max(minNumSplits, this.numSplits); 获取两种方式最大的，总之我想让任务尽可能多。但设置有2种方式
	2.计算常量文件大小。
	3.计算读取每一个数据块的最大字节数
	final long maxSplitSize = totalLength / minNumSplits + (totalLength % minNumSplits == 0 ? 0 : 1);
	总字节数/数据块数 = 每个数据块要读取多少字节(口径按照全部大小算的,但真实拆分是按照文件拆分的，所以这个值不准)
	4.固定常量，每一个文件决定了他的数据块大小。做多我也就能读取一个数据块，不能夸数据块读取。
	5.计算读取数据最小字节数
	如果设置的最小字节数<数据块常量,则有了最小字节数。
	如果没有设值最小字节数,则默认使用数据块常量。
	如果设置的最小直接输>数据块常量,则也时候数据块常量。
	比如 数据块本身64M，设置最小也要读取128M，显然是不合理的。
	6.计算数据块size拆分的最终大小
	final long splitSize = Math.max(minSplitSize, Math.min(maxSplitSize, blockSize));
	如果maxSplitSize>=blockSize,则读取的是blockSize
	如果maxSplitSize<blockSize,则读取的是blockSize的一部分。即起到了将map任务拆分的更细的作用。

	接下来控制一下，不能让拆分的数据块太小。 拆分的数据块太小了，则我就取minSplitSize
	如果minSplitSize >= value,则读取 minSplitSize
	如果minSplitSize < value,则读取 value

	因此得到的结果就是，将数据块拆分的更细。一定介于0到数据块本身之间进行拆分。
	拆分的力度受最大值和最小值控制。
	a.最小值是设置的一个常量，你可以控制.
	b.最大值受文件总大小/想要拆分的任务数  ，总大小是常量，拆分的任务数越多，则总大小越小。
	比如数据块是64M，最小值可以设置32M，最大值可以控制55M。

	如果没有设置最小值，那结果就是按照  最大值受文件总大小/想要拆分的任务数 进行拆分。
	如果设置了最小值,则结果 如果 最大值受文件总大小/想要拆分的任务数 比最小值小，则按照最小值拆分。
	还要控制 最大值受文件总大小/想要拆分的任务数 的结果不能大于数据块本身。
	0 - blocksize之间。  受任务数控制，受最小值控制。

	目标:就是拆分数据块，让他更小，可以并行更多的任务。但最差还是原来的数据块大小，即不拆分。
	1.因为每一个文件都是有不同的数据块大小的。因此能保证以文件维度下,拆分后的文件块>=拆分前。
	比如a文件有10个块，b文件有11个块，再怎么拆分，最终的结果一定是>=22块。不会小于22.最多效果都无效,即还是数据块本身大小。
	自己设置的参数都失效。
	2.如何做到的 -- 以下2选1
	1.设置最小分块大小,这个是一个固定值，所有的文件都公用同一个值，因此如果文件本身块比他大，则有效，如果文件块本身小，则该参数失效。
	2.设置任务数（必须大于1），如果任务数设置较多，甚至非常多，则拆分的数据块肯定会更多。如果任务数设置的非常小，比如1，则拆分的数据块理论上非常大，但最多也跟本身数据块相同，因此等于失效。
	3.如果2个都设置了,则看谁优先级高，如果2的结果比最小值还小，则选择最小值。如果2的结果比最小值大，则选择2.
	但无论选择1还是2，都是比原始数据块小的，最多也就与原始数据块相同。
	 */
	@Override
	public FileInputSplit[] createInputSplits(int minNumSplits) throws IOException {
		if (minNumSplits < 1) {
			throw new IllegalArgumentException("Number of input splits has to be at least 1.");
		}
		
		// take the desired number of splits into account
		//计算要拆分的数据块,虽然有建议值numSplits,但建议值也不能小于最小值minNumSplits
		minNumSplits = Math.max(minNumSplits, this.numSplits);
		
		final List<FileInputSplit> inputSplits = new ArrayList<FileInputSplit>(minNumSplits);

		// get all the files that are involved in the splits
		//要进行拆分的所有文件
		List<FileStatus> files = new ArrayList<>();
		long totalLength = 0;//总字节数

		for (Path path : getFilePaths()) {
			final FileSystem fs = path.getFileSystem();
			final FileStatus pathFile = fs.getFileStatus(path);

			if (pathFile.isDir()) {
				totalLength += addFilesInDir(path, files, true);
			} else {
				testForUnsplittable(pathFile);//校验文件是否是压缩文件,并且不能被拆分

				files.add(pathFile);
				totalLength += pathFile.getLen();
			}
		}

		// returns if unsplittable
		if (unsplittable) {//不能拆分
			int splitNum = 0;
			for (final FileStatus file : files) {
				final FileSystem fs = file.getPath().getFileSystem();
				final BlockLocation[] blocks = fs.getFileBlockLocations(file, 0, file.getLen());//文件整体所对应的数据块集合
				Set<String> hosts = new HashSet<String>();//搜集所有的host
				for(BlockLocation block : blocks) {
					hosts.addAll(Arrays.asList(block.getHosts()));
				}
				long len = file.getLen();
				if(testForUnsplittable(file)) {
					len = READ_WHOLE_SPLIT_FLAG;
				}
				FileInputSplit fis = new FileInputSplit(splitNum++, file.getPath(), 0, len,
						hosts.toArray(new String[hosts.size()]));
				inputSplits.add(fis);
			}
			return inputSplits.toArray(new FileInputSplit[inputSplits.size()]);
		}
		
		//建议每一个数据块大小
		final long maxSplitSize = totalLength / minNumSplits + (totalLength % minNumSplits == 0 ? 0 : 1);

		// now that we have the files, generate the splits
		int splitNum = 0;
		for (final FileStatus file : files) {

			final FileSystem fs = file.getPath().getFileSystem();
			final long len = file.getLen();
			final long blockSize = file.getBlockSize();
			
			final long minSplitSize;
			if (this.minSplitSize <= blockSize) {
				minSplitSize = this.minSplitSize;
			}
			else {//说明blockSize太小了,比如10M,但定义拆分blockSize的minSplitSize按照100M拆分,肯定无法拆分,所以打印异常,但让blockSize整体作为切分对象继续操作
				if (LOG.isWarnEnabled()) {
					LOG.warn("Minimal split size of " + this.minSplitSize + " is larger than the block size of " + 
						blockSize + ". Decreasing minimal split size to block size.");
				}
				minSplitSize = blockSize;
			}

			final long splitSize = Math.max(minSplitSize, Math.min(maxSplitSize, blockSize));
			final long halfSplit = splitSize >>> 1; //floor(splitSize / 2) 向下取整,比如3.7 = 3 -- 表示中间位置

			final long maxBytesForLastSplit = (long) (splitSize * MAX_SPLIT_SIZE_DISCREPANCY);

			if (len > 0) {

				// get the block locations and make sure they are in order with respect to their offset
				final BlockLocation[] blocks = fs.getFileBlockLocations(file, 0, len);
				Arrays.sort(blocks);

				long bytesUnassigned = len;
				long position = 0;

				int blockIndex = 0;

				while (bytesUnassigned > maxBytesForLastSplit) {
					// get the block containing the majority of the data
					blockIndex = getBlockIndexForPosition(blocks, position, halfSplit, blockIndex);
					// create a new split
					FileInputSplit fis = new FileInputSplit(splitNum++, file.getPath(), position, splitSize,
						blocks[blockIndex].getHosts());
					inputSplits.add(fis);

					// adjust the positions
					position += splitSize;
					bytesUnassigned -= splitSize;
				}

				// assign the last split
				if (bytesUnassigned > 0) {
					blockIndex = getBlockIndexForPosition(blocks, position, halfSplit, blockIndex);
					final FileInputSplit fis = new FileInputSplit(splitNum++, file.getPath(), position,
						bytesUnassigned, blocks[blockIndex].getHosts());
					inputSplits.add(fis);
				}
			} else {
				// special case with a file of zero bytes size
				final BlockLocation[] blocks = fs.getFileBlockLocations(file, 0, 0);
				String[] hosts;
				if (blocks.length > 0) {
					hosts = blocks[0].getHosts();
				} else {
					hosts = new String[0];
				}
				final FileInputSplit fis = new FileInputSplit(splitNum++, file.getPath(), 0, 0, hosts);
				inputSplits.add(fis);
			}
		}

		return inputSplits.toArray(new FileInputSplit[inputSplits.size()]);
	}

	/**
	 * Enumerate all files in the directory and recursive if enumerateNestedFiles is true.
	 * @return the total length of accepted files.
	 * 添加文件到files中
	 */
	private long addFilesInDir(Path path, List<FileStatus> files, boolean logExcludedFiles)
			throws IOException {
		final FileSystem fs = path.getFileSystem();

		long length = 0;

		for(FileStatus dir: fs.listStatus(path)) {
			if (dir.isDir()) {
				if (acceptFile(dir) && enumerateNestedFiles) {
					length += addFilesInDir(dir.getPath(), files, logExcludedFiles);
				} else {
					if (logExcludedFiles && LOG.isDebugEnabled()) {
						LOG.debug("Directory "+dir.getPath().toString()+" did not pass the file-filter and is excluded.");
					}
				}
			}
			else {
				if(acceptFile(dir)) {//是否接受该文件
					files.add(dir);
					length += dir.getLen();
					testForUnsplittable(dir);
				} else {
					if (logExcludedFiles && LOG.isDebugEnabled()) {
						LOG.debug("Directory "+dir.getPath().toString()+" did not pass the file-filter and is excluded.");
					}
				}
			}
		}
		return length;
	}

	//校验文件是否是压缩文件,并且不能被拆分
	protected boolean testForUnsplittable(FileStatus pathFile) {
		if(getInflaterInputStreamFactory(pathFile.getPath()) != null) {
			unsplittable = true;
			return true;
		}
		return false;
	}

	private InflaterInputStreamFactory<?> getInflaterInputStreamFactory(Path path) {
		String fileExtension = extractFileExtension(path.getName());
		if (fileExtension != null) {
			return getInflaterInputStreamFactory(fileExtension);
		} else {
			return null;
		}

	}

	/**
	 * A simple hook to filter files and directories from the input.
	 * The method may be overridden. Hadoop's FileInputFormat has a similar mechanism and applies the
	 * same filters by default.
	 * 
	 * @param fileStatus The file status to check.
	 * @return true, if the given file or directory is accepted
	 */
	public boolean acceptFile(FileStatus fileStatus) {
		final String name = fileStatus.getPath().getName();
		return !name.startsWith("_")
			&& !name.startsWith(".")
			&& !filesFilter.filterPath(fileStatus.getPath());
	}

	/**
	 * Retrieves the index of the <tt>BlockLocation</tt> that contains the part of the file described by the given
	 * offset.
	 * 
	 * @param blocks The different blocks of the file. Must be ordered by their offset.文件块集合,必须按照顺序排序
	 * @param offset The offset of the position in the file.我们要定位的字节偏移量
	 * @param startIndex The earliest index to look at.
	 * @return The index of the block containing the given position.
	 */
	private int getBlockIndexForPosition(BlockLocation[] blocks, long offset, long halfSplitSize, int startIndex) {
		// go over all indexes after the startIndex
		for (int i = startIndex; i < blocks.length; i++) {
			long blockStart = blocks[i].getOffset();
			long blockEnd = blockStart + blocks[i].getLength();

			if (offset >= blockStart && offset < blockEnd) {//找到offset 恰好属于BlockLocation的数据块
				// got the block where the split starts
				// check if the next block contains more than this one does
				// 如果下一块比这一块包含更多的数据,则选择下一个数据块,即哪个节点数据多,应该优先去哪个数据节点加载数据。反正数据是可以跨数据块读取的
				//blockEnd - offset 表示要读取的字节, 还不足1半,因此定位到下一个数据块
				if (i < blocks.length - 1 && blockEnd - offset < halfSplitSize) {//剩余数据不足一半,说明下个数据块读取的数据更多
					return i + 1;
				} else {
					return i;
				}
			}
		}
		throw new IllegalArgumentException("The given offset is not contained in the any block.");
	}
	
	// --------------------------------------------------------------------------------------------

	/**
	 * Opens an input stream to the file defined in the input format.
	 * The stream is positioned at the beginning of the given split.
	 * <p>
	 * The stream is actually opened in an asynchronous thread to make sure any interruptions to the thread 
	 * working on the input format do not reach the file system.
	 */
	@Override
	public void open(FileInputSplit fileSplit) throws IOException {

		this.currentSplit = fileSplit;
		this.splitStart = fileSplit.getStart();
		this.splitLength = fileSplit.getLength();

		if (LOG.isDebugEnabled()) {
			LOG.debug("Opening input split " + fileSplit.getPath() + " [" + this.splitStart + "," + this.splitLength + "]");
		}

		
		// open the split in an asynchronous thread
		final InputSplitOpenThread isot = new InputSplitOpenThread(fileSplit, this.openTimeout);
		isot.start();
		
		try {
			this.stream = isot.waitForCompletion();
			this.stream = decorateInputStream(this.stream, fileSplit);//可能解压缩流
		}
		catch (Throwable t) {
			throw new IOException("Error opening the Input Split " + fileSplit.getPath() + 
					" [" + splitStart + "," + splitLength + "]: " + t.getMessage(), t);
		}
		
		// get FSDataInputStream
		if (this.splitStart != 0) {//定位偏移量
			this.stream.seek(this.splitStart);
		}
	}

	/**
	 * This method allows to wrap/decorate the raw {@link FSDataInputStream} for a certain file split, e.g., for decoding.
	 * When overriding this method, also consider adapting {@link FileInputFormat#testForUnsplittable} if your
	 * stream decoration renders the input file unsplittable. Also consider calling existing superclass implementations.
	 *
	 * @param inputStream is the input stream to decorated
	 * @param fileSplit   is the file split for which the input stream shall be decorated
	 * @return the decorated input stream
	 * @throws Throwable if the decoration fails
	 * @see org.apache.flink.api.common.io.InputStreamFSInputWrapper
	 * 如果需要解压缩,则处理解压缩流
	 */
	protected FSDataInputStream decorateInputStream(FSDataInputStream inputStream, FileInputSplit fileSplit) throws Throwable {
		// Wrap stream in a extracting (decompressing) stream if file ends with a known compression file extension.
		InflaterInputStreamFactory<?> inflaterInputStreamFactory = getInflaterInputStreamFactory(fileSplit.getPath());
		if (inflaterInputStreamFactory != null) {
			return new InputStreamFSInputWrapper(inflaterInputStreamFactory.create(stream));
		}

		return inputStream;
	}

	/**
	 * Closes the file input stream of the input format.
	 */
	@Override
	public void close() throws IOException {
		if (this.stream != null) {
			// close input stream
			this.stream.close();
			stream = null;
		}
	}
	
	/**
	 * Override this method to supports multiple paths.
	 * When this method will be removed, all FileInputFormats have to support multiple paths.
	 *
	 * @return True if the FileInputFormat supports multiple paths, false otherwise.
	 *
	 * @deprecated Will be removed for Flink 2.0.
	 */
	@Deprecated
	public boolean supportsMultiPaths() {
		return false;
	}

	public String toString() {
		return getFilePaths() == null || getFilePaths().length == 0 ?
			"File Input (unknown file)" :
			"File Input (" +  Arrays.toString(this.getFilePaths()) + ')';
	}

	// ============================================================================================
	
	/**
	 * Encapsulation of the basic statistics the optimizer obtains about a file. Contained are the size of the file
	 * and the average bytes of a single record. The statistics also have a time-stamp that records the modification
	 * time of the file and indicates as such for which time the statistics were valid.
	 */
	public static class FileBaseStatistics implements BaseStatistics {
		
		protected final long fileModTime; // timestamp of the last modification 最后更新时间

		protected final long fileSize; // size of the file(s) in bytes 字节总数

		protected final float avgBytesPerRecord; // the average number of bytes for a record 每一个数据平均长度

		/**
		 * Creates a new statistics object.
		 * 
		 * @param fileModTime
		 *        The timestamp of the latest modification of any of the involved files.
		 * @param fileSize
		 *        The size of the file, in bytes. <code>-1</code>, if unknown.
		 * @param avgBytesPerRecord
		 *        The average number of byte in a record, or <code>-1.0f</code>, if unknown.
		 */
		public FileBaseStatistics(long fileModTime, long fileSize, float avgBytesPerRecord) {
			this.fileModTime = fileModTime;
			this.fileSize = fileSize;
			this.avgBytesPerRecord = avgBytesPerRecord;
		}

		/**
		 * Gets the timestamp of the last modification.
		 * 
		 * @return The timestamp of the last modification.
		 */
		public long getLastModificationTime() {
			return fileModTime;
		}

		/**
		 * Gets the file size.
		 * 
		 * @return The fileSize.
		 * @see org.apache.flink.api.common.io.statistics.BaseStatistics#getTotalInputSize()
		 */
		@Override
		public long getTotalInputSize() {
			return this.fileSize;
		}

		/**
		 * Gets the estimates number of records in the file, computed as the file size divided by the
		 * average record width, rounded up.
		 * 
		 * @return The estimated number of records in the file.
		 * @see org.apache.flink.api.common.io.statistics.BaseStatistics#getNumberOfRecords()
		 * 估算文件行数
		 */
		@Override
		public long getNumberOfRecords() {
			return (this.fileSize == SIZE_UNKNOWN || this.avgBytesPerRecord == AVG_RECORD_BYTES_UNKNOWN) ? 
				NUM_RECORDS_UNKNOWN : (long) Math.ceil(this.fileSize / this.avgBytesPerRecord);
		}

		/**
		 * Gets the estimated average number of bytes per record.
		 * 
		 * @return The average number of bytes per record.
		 * @see org.apache.flink.api.common.io.statistics.BaseStatistics#getAverageRecordWidth()
		 */
		@Override
		public float getAverageRecordWidth() {
			return this.avgBytesPerRecord;
		}
		
		@Override
		public String toString() {
			return "size=" + this.fileSize + ", recWidth=" + this.avgBytesPerRecord + ", modAt=" + this.fileModTime;
		}
	}
	
	// ============================================================================================
	
	/**
	 * Obtains a DataInputStream in an thread that is not interrupted.
	 * This is a necessary hack around the problem that the HDFS client is very sensitive to InterruptedExceptions.
	 * 如何读取一个文件数据块
	 * 获取整个文件,成本有点大啊,为什么不读取一个数据块呢? 其实他只是返回一个流,而不是真的把所有文件都返回了
	 */
	public static class InputSplitOpenThread extends Thread {
		
		private final FileInputSplit split;//待读取的文件数据块
		
		private final long timeout;//读取超时时间

		private volatile FSDataInputStream fdis;//成功读取的流信息

		private volatile Throwable error;//读取过程中出问题了
		
		private volatile boolean aborted;

		public InputSplitOpenThread(FileInputSplit split, long timeout) {
			super("Transient InputSplit Opener");
			setDaemon(true);
			
			this.split = split;
			this.timeout = timeout;
		}

		@Override
		public void run() {
			try {
				final FileSystem fs = FileSystem.get(this.split.getPath().toUri());
				this.fdis = fs.open(this.split.getPath());
				
				// check for canceling and close the stream in that case, because no one will obtain it
				if (this.aborted) {
					final FSDataInputStream f = this.fdis;
					this.fdis = null;
					f.close();
				}
			}
			catch (Throwable t) {
				this.error = t;
			}
		}
		
		public FSDataInputStream waitForCompletion() throws Throwable {
			final long start = System.currentTimeMillis();
			long remaining = this.timeout;
			
			do {
				try {
					// wait for the task completion
					this.join(remaining);
				}
				catch (InterruptedException iex) {
					// we were canceled, so abort the procedure
					abortWait();
					throw iex;
				}
			}//三个条件可退出循环  1.出问题了  2读取完成 3超时
			while (this.error == null && this.fdis == null &&
					(remaining = this.timeout + start - System.currentTimeMillis()) > 0);
			
			if (this.error != null) {//出现异常
				throw this.error;
			}
			if (this.fdis != null) {//成功返回值
				return this.fdis;
			} else {//处理超时逻辑
				// double-check that the stream has not been set by now. we don't know here whether
				// a) the opener thread recognized the canceling and closed the stream
				// b) the flag was set such that the stream did not see it and we have a valid stream
				// In any case, close the stream and throw an exception.
				abortWait();
				
				final boolean stillAlive = this.isAlive();
				final StringBuilder bld = new StringBuilder(256);
				for (StackTraceElement e : this.getStackTrace()) {
					bld.append("\tat ").append(e.toString()).append('\n');
				}
				throw new IOException("Input opening request timed out. Opener was " + (stillAlive ? "" : "NOT ") + 
					" alive. Stack of split open thread:\n" + bld.toString());
			}
		}
		
		/**
		 * Double checked procedure setting the abort flag and closing the stream.
		 */
		private void abortWait() {
			this.aborted = true;
			final FSDataInputStream inStream = this.fdis;
			this.fdis = null;
			if (inStream != null) {
				try {
					inStream.close();
				} catch (Throwable t) {}
			}
		}
	}
	
	// ============================================================================================
	//  Parameterization via configuration
	// ============================================================================================
	
	// ------------------------------------- Config Keys ------------------------------------------
	
	/**
	 * The config parameter which defines the input file path.
	 */
	private static final String FILE_PARAMETER_KEY = "input.file.path";

	/**
	 * The config parameter which defines whether input directories are recursively traversed.
	 */
	public static final String ENUMERATE_NESTED_FILES_FLAG = "recursive.file.enumeration";
}
