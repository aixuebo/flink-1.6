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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;

import org.apache.flink.shaded.guava18.com.google.common.collect.HashBasedTable;
import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableList;
import org.apache.flink.shaded.guava18.com.google.common.collect.Table;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * The result partition manager keeps track of all currently produced/consumed partitions of a
 * task manager.
 * 管理中间结果，即每一个进程+临时结果id组成的中间文件
 */
public class ResultPartitionManager implements ResultPartitionProvider {

	private static final Logger LOG = LoggerFactory.getLogger(ResultPartitionManager.class);

	//前两个组成tuple作为key,value是结果对象
	//即进程id+分区id --> 分区对象
	public final Table<ExecutionAttemptID, IntermediateResultPartitionID, ResultPartition>
			registeredPartitions = HashBasedTable.create();

	private boolean isShutdown;

	//注册一个分区对象
	public void registerResultPartition(ResultPartition partition) throws IOException {
		synchronized (registeredPartitions) {
			checkState(!isShutdown, "Result partition manager already shut down.");

			ResultPartitionID partitionId = partition.getPartitionId();

			ResultPartition previous = registeredPartitions.put(
					partitionId.getProducerId(), partitionId.getPartitionId(), partition);

			if (previous != null) {
				throw new IllegalStateException("Result partition already registered.");
			}

			LOG.debug("Registered {}.", partition);
		}
	}

	//读取某一个分区的子分区内容
	@Override
	public ResultSubpartitionView createSubpartitionView(
			ResultPartitionID partitionId,
			int subpartitionIndex,
			BufferAvailabilityListener availabilityListener) throws IOException {

		synchronized (registeredPartitions) {
			final ResultPartition partition = registeredPartitions.get(partitionId.getProducerId(),
					partitionId.getPartitionId());

			if (partition == null) {
				throw new PartitionNotFoundException(partitionId);
			}

			LOG.debug("Requesting subpartition {} of {}.", subpartitionIndex, partition);

			return partition.createSubpartitionView(subpartitionIndex, availabilityListener);
		}
	}

	//删除该executionId进程下所有结果文件
	public void releasePartitionsProducedBy(ExecutionAttemptID executionId) {
		releasePartitionsProducedBy(executionId, null);
	}

	//删除该executionId进程下所有结果文件
	public void releasePartitionsProducedBy(ExecutionAttemptID executionId, Throwable cause) {
		synchronized (registeredPartitions) {
			final Map<IntermediateResultPartitionID, ResultPartition> partitions =
					registeredPartitions.row(executionId);

			//删除executionId进程下每一个结果文件
			for (ResultPartition partition : partitions.values()) {
				partition.release(cause);
			}

			//清理内存中该executionId进程下所有的结果
			for (IntermediateResultPartitionID partitionId : ImmutableList
					.copyOf(partitions.keySet())) {

				registeredPartitions.remove(executionId, partitionId);
			}

			LOG.debug("Released all partitions produced by {}.", executionId);
		}
	}

	public void shutdown() {
		synchronized (registeredPartitions) {

			LOG.debug("Releasing {} partitions because of shutdown.",
					registeredPartitions.values().size());

			for (ResultPartition partition : registeredPartitions.values()) {
				partition.release();
			}

			registeredPartitions.clear();

			isShutdown = true;

			LOG.debug("Successful shutdown.");
		}
	}

	// ------------------------------------------------------------------------
	// Notifications
	// ------------------------------------------------------------------------

	//当partition被消费完成后,接收通知,清理该分区数据
	void onConsumedPartition(ResultPartition partition) {
		final ResultPartition previous;

		LOG.debug("Received consume notification from {}.", partition);

		synchronized (registeredPartitions) {
			ResultPartitionID partitionId = partition.getPartitionId();

			previous = registeredPartitions.remove(partitionId.getProducerId(),
					partitionId.getPartitionId());
		}

		// Release the partition if it was successfully removed
		if (partition == previous) {
			partition.release();

			LOG.debug("Released {}.", partition);
		}
	}
}
