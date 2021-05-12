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

package org.apache.flink.core.fs;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;

/**
 * The RecoverableWriter creates and recovers {@link RecoverableFsDataOutputStream}.
 * It can be used to write data to a file system in a way that the writing can be
 * resumed consistently after a failure and recovery without loss of data or possible
 * duplication of bytes.
 *
 * <p>The streams do not make the files they write to immediately visible, but instead write
 * to temp files or other temporary storage. To publish the data atomically in the
 * end, the stream offers the {@link RecoverableFsDataOutputStream#closeForCommit()} method
 * to create a committer that publishes the result.
 *
 * <p>These writers are useful in the context of checkpointing. The example below illustrates
 * how to use them:
 *
 * <pre>{@code
 * // --------- initial run --------
 * RecoverableWriter writer = fileSystem.createRecoverableWriter();
 * RecoverableFsDataOutputStream out = writer.open(path);
 * out.write(...);
 *
 * // persist intermediate state
 * ResumeRecoverable intermediateState = out.persist();
 * storeInCheckpoint(intermediateState);
 *
 * // --------- recovery --------
 * ResumeRecoverable lastCheckpointState = ...; // get state from checkpoint
 * RecoverableWriter writer = fileSystem.createRecoverableWriter();
 * RecoverableFsDataOutputStream out = writer.recover(lastCheckpointState);
 *
 * out.write(...); // append more data
 *
 * out.closeForCommit().commit(); // close stream and publish all the data
 *
 * // --------- recovery without appending --------
 * ResumeRecoverable lastCheckpointState = ...; // get state from checkpoint
 * RecoverableWriter writer = fileSystem.createRecoverableWriter();
 * Committer committer = writer.recoverForCommit(lastCheckpointState);
 * committer.commit(); // publish the state as of the last checkpoint
 * }</pre>
 *
 * <h3>Recovery</h3>
 *
 * <p>Recovery relies on data persistence in the target file system or object store. While the
 * code itself works with the specific primitives that the target storage offers, recovery will
 * fail if the data written so far was deleted by an external factor.
 * For example, some implementations stage data in temp files or object parts. If these
 * were deleted by someone or by an automated cleanup policy, then resuming
 * may fail. This is not surprising and should be expected, but we want to explicitly point
 * this out here.
 *
 * <p>Specific care is needed for systems like S3, where the implementation uses Multipart Uploads
 * to incrementally upload and persist parts of the result. Timeouts for Multipart Uploads
 * and life time of Parts in unfinished Multipart Uploads need to be set in the bucket policy
 * high enough to accommodate the recovery. These values are typically in the days, so regular
 * recovery is typically not a problem. What can become an issue is situations where a Flink
 * application is hard killed (all processes or containers removed) and then one tries to
 * manually recover the application from an externalized checkpoint some days later. In that
 * case, systems like S3 may have removed uncommitted parts and recovery will not succeed.
 *
 * <h3>Implementer's Note</h3>
 *
 * <p>From the perspective of the implementer, it would be desirable to make this class
 * generic with respect to the concrete types of 'CommitRecoverable' and 'ResumeRecoverable'.
 * However, we found that this makes the code more clumsy to use and we hence dropped the
 * generics at the cost of doing some explicit casts in the implementation that would
 * otherwise have been implicitly generated by the generics compiler.
 * 创建一个可恢复的输出流,在加工数据的时候,将一些结果存储到该输出流中,该输出流最后可以替换掉原始的文件
 */
public interface RecoverableWriter {

	/**
	 * Opens a new recoverable stream to write to the given path.
	 * Whether existing files will be overwritten is implementation specific and should
	 * not be relied upon.
	 *
	 * @param path The path of the file/object to write to.
	 * @return A new RecoverableFsDataOutputStream writing a new file/object.
	 *
	 * @throws IOException Thrown if the stream could not be opened/initialized.
	 * 打开一个可恢复的输出流
	 */
	RecoverableFsDataOutputStream open(Path path) throws IOException;

	/**
	 * Resumes a recoverable stream consistently at the point indicated by the given ResumeRecoverable.
	 * Future writes to the stream will continue / append the file as of that point.
	 *
	 * <p>This method is optional and whether it is supported is indicated through the
	 * {@link #supportsResume()} method.
	 *
	 * @param resumable The opaque handle with the recovery information.
	 * @return A recoverable stream writing to the file/object as it was at the point when the
	 *         ResumeRecoverable was created.
	 *
	 * @throws IOException Thrown, if resuming fails.
	 * @throws UnsupportedOperationException Thrown if this optional method is not supported.
	 * 针对一个可恢复的文件,创建输出流
	 */
	RecoverableFsDataOutputStream recover(ResumeRecoverable resumable) throws IOException;

	/**
	 * Recovers a recoverable stream consistently at the point indicated by the given CommitRecoverable
	 * for finalizing and committing. This will publish the target file with exactly the data
	 * that was written up to the point then the CommitRecoverable was created.
	 *
	 * @param resumable The opaque handle with the recovery information.
	 * @return A committer that publishes the target file.
	 *
	 * @throws IOException Thrown, if recovery fails.
	 */
	RecoverableFsDataOutputStream.Committer recoverForCommit(CommitRecoverable resumable) throws IOException;

	/**
	 * The serializer for the CommitRecoverable types created in this writer.
	 * This serializer should be used to store the CommitRecoverable in checkpoint
	 * state or other forms of persistent state.
	 */
	SimpleVersionedSerializer<CommitRecoverable> getCommitRecoverableSerializer();

	/**
	 * The serializer for the ResumeRecoverable types created in this writer.
	 * This serializer should be used to store the ResumeRecoverable in checkpoint
	 * state or other forms of persistent state.
	 */
	SimpleVersionedSerializer<ResumeRecoverable> getResumeRecoverableSerializer();

	/**
	 * Checks whether the writer and its streams support resuming (appending to) files after
	 * recovery (via the {@link #recover(ResumeRecoverable)} method).
	 *
	 * <p>If true, then this writer supports the {@link #recover(ResumeRecoverable)} method.
	 * If false, then that method may not be supported and streams can only be recovered via
	 * {@link #recoverForCommit(CommitRecoverable)}.
	 * 是否支持恢复
	 */
	boolean supportsResume();

	// ------------------------------------------------------------------------

	/**
	 * A handle to an in-progress stream with a defined and persistent amount of data.
	 * The handle can be used to recover the stream as of exactly that point and
	 * publish the result file.
	 * 提交可恢复的
	 * 参见:LocalRecoverable
	 * 什么是可恢复---即在处理某一个文件的时候，记录一些元数据，通过元数据可以恢复重新消费的能力
	 * 感觉用在checkpoint上最合适
	 */
	interface CommitRecoverable {}

	/**
	 * A handle to an in-progress stream with a defined and persistent amount of data.
	 * The handle can be used to recover the stream exactly as of that point and either
	 * publish the result file or keep appending data to the stream.
	 * 重新消费可恢复的
	 * 参见:LocalRecoverable
	 * 什么是可恢复---即在处理某一个文件的时候，记录一些元数据，通过元数据可以恢复重新消费的能力
	 * 感觉用在checkpoint上最合适
	 */
	interface ResumeRecoverable extends CommitRecoverable {}
}
