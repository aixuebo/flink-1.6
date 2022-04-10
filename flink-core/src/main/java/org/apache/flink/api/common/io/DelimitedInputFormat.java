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
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.OptimizerOptions;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.parser.FieldParser;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Base implementation for input formats that split the input at a delimiter into records.
 * The parsing of the record bytes into the record has to be implemented in the
 * {@link #readRecord(Object, byte[], int, int)} method.
 * 
 * <p>The default delimiter is the newline character {@code '\n'}.</p>
 */
@Public
public abstract class DelimitedInputFormat<OT> extends FileInputFormat<OT> implements CheckpointableInputFormat<FileInputSplit, Long> {
	
	private static final long serialVersionUID = 1L;

	// -------------------------------------- Constants -------------------------------------------
	
	/**
	 * The log.
	 */
	private static final Logger LOG = LoggerFactory.getLogger(DelimitedInputFormat.class);

	// The charset used to convert strings to bytes
	private String charsetName = "UTF-8";

	// Charset is not serializable
	private transient Charset charset;

	/**
	 * The default read buffer size = 1MB.
	 */
	private static final int DEFAULT_READ_BUFFER_SIZE = 1024 * 1024;
	
	/**
	 * Indication that the number of samples has not been set by the configuration.
	 * 抽样函数默认值
	 */
	private static final int NUM_SAMPLES_UNDEFINED = -1;
	
	/**
	 * The maximum number of line samples to be taken.
	 * 最大抽样数
	 */
	private static int DEFAULT_MAX_NUM_SAMPLES;
	
	/**
	 * The minimum number of line samples to be taken.
	 * 最小抽样数
	 */
	private static int DEFAULT_MIN_NUM_SAMPLES;
	
	/**
	 * The maximum size of a sample record before sampling is aborted. To catch cases where a wrong delimiter is given.
	 * 最大的抽样数据中,字节长度
	 */
	private static int MAX_SAMPLE_LEN;

	/**
	 * @deprecated Please use {@code loadConfigParameters(Configuration config}
	 */
	@Deprecated
	protected static void loadGlobalConfigParams() {
		loadConfigParameters(GlobalConfiguration.loadConfiguration());
	}

	//设置最大抽样数、最小抽样数、每条数据最大字节数
	protected static void loadConfigParameters(Configuration parameters) {
		int maxSamples = parameters.getInteger(OptimizerOptions.DELIMITED_FORMAT_MAX_LINE_SAMPLES);
		int minSamples = parameters.getInteger(OptimizerOptions.DELIMITED_FORMAT_MIN_LINE_SAMPLES);
		
		if (maxSamples < 0) {
			LOG.error("Invalid default maximum number of line samples: " + maxSamples + ". Using default value of " +
				OptimizerOptions.DELIMITED_FORMAT_MAX_LINE_SAMPLES.key());
			maxSamples = OptimizerOptions.DELIMITED_FORMAT_MAX_LINE_SAMPLES.defaultValue();
		}
		if (minSamples < 0) {
			LOG.error("Invalid default minimum number of line samples: " + minSamples + ". Using default value of " +
				OptimizerOptions.DELIMITED_FORMAT_MIN_LINE_SAMPLES.key());
			minSamples = OptimizerOptions.DELIMITED_FORMAT_MIN_LINE_SAMPLES.defaultValue();
		}
		
		DEFAULT_MAX_NUM_SAMPLES = maxSamples;
		
		if (minSamples > maxSamples) {
			LOG.error("Default minimum number of line samples cannot be greater the default maximum number " +
					"of line samples: min=" + minSamples + ", max=" + maxSamples + ". Defaulting minimum to maximum.");
			DEFAULT_MIN_NUM_SAMPLES = maxSamples;
		} else {
			DEFAULT_MIN_NUM_SAMPLES = minSamples;
		}
		
		int maxLen = parameters.getInteger(OptimizerOptions.DELIMITED_FORMAT_MAX_SAMPLE_LEN);
		if (maxLen <= 0) {
			maxLen = OptimizerOptions.DELIMITED_FORMAT_MAX_SAMPLE_LEN.defaultValue();
			LOG.error("Invalid value for the maximum sample record length. Using default value of " + maxLen + '.');
		} else if (maxLen < DEFAULT_READ_BUFFER_SIZE) {
			maxLen = DEFAULT_READ_BUFFER_SIZE;
			LOG.warn("Increasing maximum sample record length to size of the read buffer (" + maxLen + ").");
		}
		MAX_SAMPLE_LEN = maxLen;
	}
	
	// --------------------------------------------------------------------------------------------
	//  Variables for internal parsing.
	//  They are all transient, because we do not want them so be serialized 
	// --------------------------------------------------------------------------------------------
	
	private transient byte[] readBuffer;//用于存储从流中读取的数据

	private transient byte[] wrapBuffer;//存储当readBuffer中的内容还没有到一行数据,因此部分数据要存储起来

	private transient int readPos;//readBuffer中目前准备消费的位置
	private transient int limit;//readBuffer中end位置

	private transient byte[] currBuffer;		// buffer in which current record byte sequence is found 存储当前一行记录的缓冲区
	private transient int currOffset;			// offset in above buffer
	private transient int currLen;				// length of current byte sequence 当前行大小

	private transient boolean overLimit;

	private transient boolean end;//表示是否已经读取结尾

	private long offset = -1; //表示一行数据的offset开始位置 --- 在整体的文件中

	// --------------------------------------------------------------------------------------------
	//  The configuration parameters. Configured on the instance and serialized to be shipped.
	// --------------------------------------------------------------------------------------------

	// The delimiter may be set with a byte-sequence or a String. In the latter
	// case the byte representation is updated consistent with current charset.
	//如果不想是回车作为换行符,可以set替换
	private byte[] delimiter = new byte[] {'\n'};//delimiterString的字节数组  一条数据的分隔符
	private String delimiterString = null;

	private int lineLengthLimit = Integer.MAX_VALUE;//一行数据最多允许多少个字节
	
	private int bufferSize = -1;
	
	private int numLineSamples = NUM_SAMPLES_UNDEFINED;//抽样行数
	
	
	// --------------------------------------------------------------------------------------------
	//  Constructors & Getters/setters for the configurable parameters
	// --------------------------------------------------------------------------------------------

	public DelimitedInputFormat() {
		this(null, null);
	}

	protected DelimitedInputFormat(Path filePath, Configuration configuration) {
		super(filePath);
		if (configuration == null) {
			configuration = GlobalConfiguration.loadConfiguration();
		}
		loadConfigParameters(configuration);
	}

	/**
	 * Get the character set used for the row delimiter. This is also used by
	 * subclasses to interpret field delimiters, comment strings, and for
	 * configuring {@link FieldParser}s.
	 *
	 * @return the charset
	 */
	@PublicEvolving
	public Charset getCharset() {
		if (this.charset == null) {
			this.charset = Charset.forName(charsetName);
		}
		return this.charset;
	}

	/**
	 * Set the name of the character set used for the row delimiter. This is
	 * also used by subclasses to interpret field delimiters, comment strings,
	 * and for configuring {@link FieldParser}s.
	 *
	 * These fields are interpreted when set. Changing the charset thereafter
	 * may cause unexpected results.
	 *
	 * @param charset name of the charset
	 */
	@PublicEvolving
	public void setCharset(String charset) {
		this.charsetName = Preconditions.checkNotNull(charset);
		this.charset = null;

		if (this.delimiterString != null) {
			this.delimiter = delimiterString.getBytes(getCharset());
		}
	}

	public byte[] getDelimiter() {
		return delimiter;
	}
	
	public void setDelimiter(byte[] delimiter) {
		if (delimiter == null) {
			throw new IllegalArgumentException("Delimiter must not be null");
		}
		this.delimiter = delimiter;
		this.delimiterString = null;
	}

	public void setDelimiter(char delimiter) {
		setDelimiter(String.valueOf(delimiter));
	}
	
	public void setDelimiter(String delimiter) {
		if (delimiter == null) {
			throw new IllegalArgumentException("Delimiter must not be null");
		}
		this.delimiter = delimiter.getBytes(getCharset());
		this.delimiterString = delimiter;
	}
	
	public int getLineLengthLimit() {
		return lineLengthLimit;
	}
	
	public void setLineLengthLimit(int lineLengthLimit) {
		if (lineLengthLimit < 1) {
			throw new IllegalArgumentException("Line length limit must be at least 1.");
		}

		this.lineLengthLimit = lineLengthLimit;
	}
	
	public int getBufferSize() {
		return bufferSize;
	}
	
	public void setBufferSize(int bufferSize) {
		if (bufferSize < 2) {
			throw new IllegalArgumentException("Buffer size must be at least 2.");
		}

		this.bufferSize = bufferSize;
	}
	
	public int getNumLineSamples() {
		return numLineSamples;
	}
	
	public void setNumLineSamples(int numLineSamples) {
		if (numLineSamples < 0) {
			throw new IllegalArgumentException("Number of line samples must not be negative.");
		}
		this.numLineSamples = numLineSamples;
	}

	// --------------------------------------------------------------------------------------------
	//  User-defined behavior
	// --------------------------------------------------------------------------------------------

	/**
	 * This function parses the given byte array which represents a serialized record.
	 * The function returns a valid record or throws an IOException.
	 * 
	 * @param reuse An optionally reusable object.
	 * @param bytes Binary data of serialized records.
	 * @param offset The offset where to start to read the record data. 
	 * @param numBytes The number of bytes that can be read starting at the offset position.
	 * 
	 * @return Returns the read record if it was successfully deserialized.
	 * @throws IOException if the record could not be read.
	 */
	public abstract OT readRecord(OT reuse, byte[] bytes, int offset, int numBytes) throws IOException;
	
	// --------------------------------------------------------------------------------------------
	//  Pre-flight: Configuration, Splits, Sampling
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Configures this input format by reading the path to the file from the configuration and the string that
	 * defines the record delimiter.
	 * 
	 * @param parameters The configuration object to read the parameters from.
	 * 配置抽样行数、换行分隔符
	 */
	@Override
	public void configure(Configuration parameters) {
		super.configure(parameters);

		// the if() clauses are to prevent the configure() method from
		// overwriting the values set by the setters

		if (Arrays.equals(delimiter, new byte[] {'\n'})) {
			String delimString = parameters.getString(RECORD_DELIMITER, null);
			if (delimString != null) {
				setDelimiter(delimString);
			}
		}
		
		// set the number of samples
		if (numLineSamples == NUM_SAMPLES_UNDEFINED) {
			String samplesString = parameters.getString(NUM_STATISTICS_SAMPLES, null);
			if (samplesString != null) {
				try {
					setNumLineSamples(Integer.parseInt(samplesString));
				} catch (NumberFormatException e) {
					if (LOG.isWarnEnabled()) {
						LOG.warn("Invalid value for number of samples to take: " + samplesString + ". Skipping sampling.");
					}
					setNumLineSamples(0);
				}
			}
		}
	}

	//抽样统计每行字节长度
	@Override
	public FileBaseStatistics getStatistics(BaseStatistics cachedStats) throws IOException {
		
		final FileBaseStatistics cachedFileStats = cachedStats instanceof FileBaseStatistics ?
				(FileBaseStatistics) cachedStats : null;
		
		// store properties
		final long oldTimeout = this.openTimeout;
		final int oldBufferSize = this.bufferSize;
		final int oldLineLengthLimit = this.lineLengthLimit;
		try {

			final ArrayList<FileStatus> allFiles = new ArrayList<>(1);

			// let the file input format deal with the up-to-date check and the basic size
			final FileBaseStatistics stats = getFileStats(cachedFileStats, getFilePaths(), allFiles);
			if (stats == null) {
				return null;
			}
			
			// check whether the width per record is already known or the total size is unknown as well
			// in both cases, we return the stats as they are
			if (stats.getAverageRecordWidth() != FileBaseStatistics.AVG_RECORD_BYTES_UNKNOWN ||
					stats.getTotalInputSize() == FileBaseStatistics.SIZE_UNKNOWN) {
				return stats;
			}

			// disabling sampling for unsplittable files since the logic below assumes splitability.
			// TODO: Add sampling for unsplittable files. Right now, only compressed text files are affected by this limitation.
			//不能拆分文件不统计,属于压缩文件
			if (unsplittable) {
				return stats;
			}
			
			// compute how many samples to take, depending on the defined upper and lower bound
			final int numSamples;//抽样行数
			if (this.numLineSamples != NUM_SAMPLES_UNDEFINED) {
				numSamples = this.numLineSamples;
			} else {
				//初始化抽样条数,按照每1k字节抽取一条计算
				// make the samples small for very small files
				final int calcSamples = (int) (stats.getTotalInputSize() / 1024);//假设1k字节中抽取一条数据.需要抽取多少条
				numSamples = Math.min(DEFAULT_MAX_NUM_SAMPLES, Math.max(DEFAULT_MIN_NUM_SAMPLES, calcSamples));
			}
			
			// check if sampling is disabled.
			if (numSamples == 0) {
				return stats;
			}
			if (numSamples < 0) {
				throw new RuntimeException("Error: Invalid number of samples: " + numSamples);
			}
			
			
			// make sure that the sampling times out after a while if the file system does not answer in time
			this.openTimeout = 10000;
			// set a small read buffer size
			this.bufferSize = 4 * 1024;
			// prevent overly large records, for example if we have an incorrectly configured delimiter
			this.lineLengthLimit = MAX_SAMPLE_LEN;
			
			long offset = 0;//下一次抽样的位置
			long totalNumBytes = 0;//抽样多少字节
			long stepSize = stats.getTotalInputSize() / numSamples;//步长

			int fileNum = 0;//当前第几个文件
			int samplesTaken = 0;//第几个抽样

			// take the samples
			while (samplesTaken < numSamples && fileNum < allFiles.size()) {//抽样
				// make a split for the sample and use it to read a record
				FileStatus file = allFiles.get(fileNum);
				//从offset开始读取数据,能读取的长度已经不是文件的总长度了,因为已经有offset不能再读取了,所以是file.getLen() - offset。
				//不存在数据块编号和host的需求,因此不设置
				FileInputSplit split = new FileInputSplit(0, file.getPath(), offset, file.getLen() - offset, null);

				// we open the split, read one line, and take its length
				try {
					open(split);
					if (readLine()) {
						totalNumBytes += this.currLen + this.delimiter.length;//计算一行的字节大小 --- 包含换行符
						samplesTaken++;
					}
				} finally {
					// close the file stream, do not release the buffers
					super.close();
				}

				offset += stepSize;

				// skip to the next file, if necessary
				while (fileNum < allFiles.size() && offset >= (file = allFiles.get(fileNum)).getLen()) {
					offset -= file.getLen();
					fileNum++;
				}
			}
			
			// we have the width, store it
			return new FileBaseStatistics(stats.getLastModificationTime(),
				stats.getTotalInputSize(), totalNumBytes / (float) samplesTaken);//抽样的字节数/抽样的行数
			
		} catch (IOException ioex) {
			if (LOG.isWarnEnabled()) {
				LOG.warn("Could not determine statistics for files '" + Arrays.toString(getFilePaths()) + "' " +
						 "due to an io error: " + ioex.getMessage());
			}
		}
		catch (Throwable t) {
			if (LOG.isErrorEnabled()) {
				LOG.error("Unexpected problem while getting the file statistics for files '" + Arrays.toString(getFilePaths()) + "': "
						+ t.getMessage(), t);
			}
		} finally {
			// restore properties (even on return)
			this.openTimeout = oldTimeout;
			this.bufferSize = oldBufferSize;
			this.lineLengthLimit = oldLineLengthLimit;
		}
		
		// no statistics possible
		return null;
	}

	/**
	 * Opens the given input split. This method opens the input stream to the specified file, allocates read buffers
	 * and positions the stream at the correct position, making sure that any partial record at the beginning is skipped.
	 *
	 * @param split The input split to open.
	 *
	 * @see org.apache.flink.api.common.io.FileInputFormat#open(org.apache.flink.core.fs.FileInputSplit)
	 */
	@Override
	public void open(FileInputSplit split) throws IOException {
		super.open(split);
		initBuffers();

		this.offset = splitStart;//文件块的开始位置
		if (this.splitStart != 0) {
			this.stream.seek(offset);//先定位位置,在填充数据
			//填充fillBuffer数据,删除上一行的内容
			readLine();//将这一行数据删除掉,因为定位的offset不一定是行文件的开始---丢到的这部分数据,肯定被上一个分块的最后一行数据读取完成了
			// if the first partial record already pushes the stream over
			// the limit of our split, then no record starts within this split
			if (this.overLimit) {
				this.end = true;
			}
		} else {
			fillBuffer(0);//从头读,因此直接填充数据
		}
	}

	//初始化
	private void initBuffers() {
		this.bufferSize = this.bufferSize <= 0 ? DEFAULT_READ_BUFFER_SIZE : this.bufferSize;

		if (this.bufferSize <= this.delimiter.length) {
			throw new IllegalArgumentException("Buffer size must be greater than length of delimiter.");
		}

		if (this.readBuffer == null || this.readBuffer.length != this.bufferSize) {
			this.readBuffer = new byte[this.bufferSize];
		}
		if (this.wrapBuffer == null || this.wrapBuffer.length < 256) {
			this.wrapBuffer = new byte[256];
		}

		this.readPos = 0;
		this.limit = 0;
		this.overLimit = false;
		this.end = false;
	}

	/**
	 * Checks whether the current split is at its end.
	 * 
	 * @return True, if the split is at its end, false otherwise.
	 */
	@Override
	public boolean reachedEnd() {
		return this.end;
	}

	// 参数是可以重复利用的对象.比如String,可以重复利用该对象,每次往对象中赋予新值即可
	// 如果返回null,说明读取结束
	@Override
	public OT nextRecord(OT record) throws IOException {
		if (readLine()) {
			return readRecord(record, this.currBuffer, this.currOffset, this.currLen);
		} else {
			this.end = true;
			return null;
		}
	}

	/**
	 * Closes the input by releasing all buffers and closing the file input stream.
	 * 
	 * @throws IOException Thrown, if the closing of the file stream causes an I/O error.
	 */
	@Override
	public void close() throws IOException {
		this.wrapBuffer = null;
		this.readBuffer = null;
		super.close();
	}

	// --------------------------------------------------------------------------------------------

	protected final boolean readLine() throws IOException {
		if (this.stream == null || this.overLimit) {
			return false;
		}

		int countInWrapBuffer = 0;

		// position of matching positions in the delimiter byte array
		int delimPos = 0;//分解符字节数组匹配的位置,如果分解符不是一个字节,而是5个字节,则必须要求全部匹配,则delimPos需要从0到4必须依次移动都存在

		while (true) {
			if (this.readPos >= this.limit) {//填充数据
				// readBuffer is completely consumed. Fill it again but keep partially read delimiter bytes.
				if (!fillBuffer(delimPos)) {//读取流结束
					int countInReadBuffer = delimPos;
					if (countInWrapBuffer + countInReadBuffer > 0) {//说明还有数据
						// we have bytes left to emit
						if (countInReadBuffer > 0) {//说明有一部分换行符出现了
							// we have bytes left in the readBuffer. Move them into the wrapBuffer
							if (this.wrapBuffer.length - countInWrapBuffer < countInReadBuffer) {//wrapBuffer依然不足存储 剩余的换行符内容
								// reallocate
								byte[] tmp = new byte[countInWrapBuffer + countInReadBuffer];//继续扩容成正好的字节数组
								System.arraycopy(this.wrapBuffer, 0, tmp, 0, countInWrapBuffer);
								this.wrapBuffer = tmp;
							}

							// copy readBuffer bytes to wrapBuffer 将readBuffer的数据继续追加到wrapBuffer中
							System.arraycopy(this.readBuffer, 0, this.wrapBuffer, countInWrapBuffer, countInReadBuffer);
							countInWrapBuffer += countInReadBuffer;
						}

						this.offset += countInWrapBuffer;//偏移量继续移动
						setResult(this.wrapBuffer, 0, countInWrapBuffer);
						return true;
					} else {
						return false;
					}
				}
			}

			int startPos = this.readPos - delimPos;
			int count;

			// Search for next occurrence of delimiter in read buffer.在buffer中搜索下一个分解符位置
			while (this.readPos < this.limit && delimPos < this.delimiter.length) {
				if ((this.readBuffer[this.readPos]) == this.delimiter[delimPos]) {
					// Found the expected delimiter character. Continue looking for the next character of delimiter.
					delimPos++;
				} else {
					// Delimiter does not match.说明不是全部匹配
					// We have to reset the read position to the character after the first matching character
					//   and search for the whole delimiter again.
					readPos -= delimPos;//跳过第一个匹配的位置,继续查找
					delimPos = 0;
				}
				readPos++;
			}

			// check why we dropped out
			if (delimPos == this.delimiter.length) {//说明在现有的buffer中找到了换行符
				// we found a delimiter
				int readBufferBytesRead = this.readPos - startPos;//遇到换行符时,读取了多少个字节
				this.offset += countInWrapBuffer + readBufferBytesRead;
				count = readBufferBytesRead - this.delimiter.length;//刨除换行字节长度

				// copy to byte array
				if (countInWrapBuffer > 0) {
					// check wrap buffer size
					if (this.wrapBuffer.length < countInWrapBuffer + count) {
						final byte[] nb = new byte[countInWrapBuffer + count];
						System.arraycopy(this.wrapBuffer, 0, nb, 0, countInWrapBuffer);
						this.wrapBuffer = nb;
					}
					if (count >= 0) {
						System.arraycopy(this.readBuffer, 0, this.wrapBuffer, countInWrapBuffer, count);
					}
					setResult(this.wrapBuffer, 0, countInWrapBuffer + count);
					return true;
				} else {
					setResult(this.readBuffer, startPos, count);
					return true;
				}
			} else {//说明现有的buffer中没有找到换行符
				// we reached the end of the readBuffer
				count = this.limit - startPos;//目前已经读取了多少个字节
				
				// check against the maximum record length
				if (((long) countInWrapBuffer) + count > this.lineLengthLimit) {//超过了最大允许字节限制,抛异常,说明数据太长了
					throw new IOException("The record length exceeded the maximum record length (" + 
							this.lineLengthLimit + ").");
				}

				// Compute number of bytes to move to wrapBuffer
				// Chars of partially read delimiter must remain in the readBuffer. We might need to go back.
				int bytesToMove = count - delimPos;//要填充到缓冲区的数据内容
				// ensure wrapBuffer is large enough
				if (this.wrapBuffer.length - countInWrapBuffer < bytesToMove) {//缓冲区不足
					// reallocate
					byte[] tmp = new byte[Math.max(this.wrapBuffer.length * 2, countInWrapBuffer + bytesToMove)];
					System.arraycopy(this.wrapBuffer, 0, tmp, 0, countInWrapBuffer);
					this.wrapBuffer = tmp;//扩展缓存区
				}

				// copy readBuffer to wrapBuffer (except delimiter chars) 移除可能的换行分隔符字节内容
				System.arraycopy(this.readBuffer, startPos, this.wrapBuffer, countInWrapBuffer, bytesToMove);
				countInWrapBuffer += bytesToMove;
				// move delimiter chars to the beginning of the readBuffer 将可能的换行符内容,添加到buffer中
				System.arraycopy(this.readBuffer, this.readPos - delimPos, this.readBuffer, 0, delimPos);

			}
		}
	}

	//len不包含换行分隔符字节
	private void setResult(byte[] buffer, int offset, int len) {
		this.currBuffer = buffer;
		this.currOffset = offset;
		this.currLen = len;
	}

	/**
	 * Fills the read buffer with bytes read from the file starting from an offset.
	 * 从offset位置开始填充数据,offset之前的数据保留
	 */
	private boolean fillBuffer(int offset) throws IOException {
		int maxReadLength = this.readBuffer.length - offset;
		// special case for reading the whole split.
		if (this.splitLength == FileInputFormat.READ_WHOLE_SPLIT_FLAG) {
			int read = this.stream.read(this.readBuffer, offset, maxReadLength);//向readBuffer中写入数据,写入maxReadLength个数据,从offset开始写入
			if (read == -1) {
				this.stream.close();
				this.stream = null;
				return false;
			} else {
				this.readPos = offset;
				this.limit = read;
				return true;
			}
		}
		
		// else ..
		int toRead;//计算要读取多少字节,因为可能readBuffer非常大,比要读取的splitLength还要大
		if (this.splitLength > 0) {
			// if we have more data, read that
			toRead = this.splitLength > maxReadLength ? maxReadLength : (int) this.splitLength;
		}
		else {
			// if we have exhausted our split, we need to complete the current record, or read one
			// more across the next split.
			// the reason is that the next split will skip over the beginning until it finds the first
			// delimiter, discarding it as an incomplete chunk of data that belongs to the last record in the
			// previous split.
			toRead = maxReadLength;
			this.overLimit = true;
		}

		int read = this.stream.read(this.readBuffer, offset, toRead);//真正读取数据

		if (read == -1) {
			this.stream.close();
			this.stream = null;
			return false;
		} else {
			this.splitLength -= read;
			this.readPos = offset; // position from where to start reading
			this.limit = read + offset; // number of valid bytes in the read buffer
			return true;
		}
	}

	// --------------------------------------------------------------------------------------------
	// Config Keys for Parametrization via configuration
	// --------------------------------------------------------------------------------------------

	/**
	 * The configuration key to set the record delimiter.
	 * 配置行分隔符
	 */
	protected static final String RECORD_DELIMITER = "delimited-format.delimiter";
	
	/**
	 * The configuration key to set the number of samples to take for the statistics.
	 * 抽样的配置key
	 */
	private static final String NUM_STATISTICS_SAMPLES = "delimited-format.numSamples";

	// --------------------------------------------------------------------------------------------
	//  Checkpointing
	// --------------------------------------------------------------------------------------------

	@PublicEvolving
	@Override
	public Long getCurrentState() throws IOException {
		return this.offset;
	}

	//重新打开一个数据块,从state状态位置中开始读取数据
	@PublicEvolving
	@Override
	public void reopen(FileInputSplit split, Long state) throws IOException {
		Preconditions.checkNotNull(split, "reopen() cannot be called on a null split.");
		Preconditions.checkNotNull(state, "reopen() cannot be called with a null initial state.");
		Preconditions.checkArgument(state == -1 || state >= split.getStart(),
			" Illegal offset "+ state +", smaller than the splits start=" + split.getStart());

		try {
			this.open(split);//虽然读取的时候,offset已经被赋值,可能是错误的,因此需要重新设置offset
		} finally {
			this.offset = state;//重新设置位置
		}

		if (state > this.splitStart + split.getLength()) {
			this.end = true;
		} else if (state > split.getStart()) {
			initBuffers();

			this.stream.seek(this.offset);
			if (split.getLength() == -1) {
				// this is the case for unsplittable files
				fillBuffer(0);
			} else {
				this.splitLength = this.splitStart + split.getLength() - this.offset;//重新设计要读取的字节长度
				if (splitLength <= 0) {
					this.end = true;
				}
			}
		}
	}
}
