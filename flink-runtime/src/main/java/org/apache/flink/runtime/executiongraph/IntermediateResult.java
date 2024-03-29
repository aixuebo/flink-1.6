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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * 由于ExecutionJobVertex表示任务计算操作节点,但他是有并行度的,因此每一个并行产生的结果就是IntermediateResultPartition
 */
public class IntermediateResult {

	private final IntermediateDataSetID id;//结果ID

	private final ExecutionJobVertex producer;//哪个task产生的结果

	private final IntermediateResultPartition[] partitions;//task上产生的结果会分发给N个partition,因此会持有每一个partition信息

	/**
	 * Maps intermediate result partition IDs to a partition index. This is
	 * used for ID lookups of intermediate results. I didn't dare to change the
	 * partition connect logic in other places that is tightly coupled to the
	 * partitions being held as an array.
	 *
	 * key是partition唯一ID,value是该partition归属第几个partition
	 *
	 * 主要是partition结果用数组存储的，而不是Map,因此需要额外维护这个关系
	 */
	private final HashMap<IntermediateResultPartitionID, Integer> partitionLookupHelper = new HashMap<>();

	private final int numParallelProducers;//多少个partiiton分区

	private final AtomicInteger numberOfRunningProducers;//有多少个分区在运行中

	private int partitionsAssigned;

	private int numConsumers;

	private final int connectionIndex;

	private final ResultPartitionType resultType;

	public IntermediateResult(
			IntermediateDataSetID id,
			ExecutionJobVertex producer,
			int numParallelProducers,
			ResultPartitionType resultType) {

		this.id = checkNotNull(id);
		this.producer = checkNotNull(producer);

		checkArgument(numParallelProducers >= 1);
		this.numParallelProducers = numParallelProducers;

		this.partitions = new IntermediateResultPartition[numParallelProducers];

		this.numberOfRunningProducers = new AtomicInteger(numParallelProducers);

		// we do not set the intermediate result partitions here, because we let them be initialized by
		// the execution vertex that produces them

		// assign a random connection index
		this.connectionIndex = (int) (Math.random() * Integer.MAX_VALUE);

		// The runtime type for this produced result
		this.resultType = checkNotNull(resultType);
	}

	//设置一个partition子结果
	public void setPartition(int partitionNumber, IntermediateResultPartition partition) {
		if (partition == null || partitionNumber < 0 || partitionNumber >= numParallelProducers) {
			throw new IllegalArgumentException();
		}

		//必须是null,即不能有结果已经分配
		if (partitions[partitionNumber] != null) {
			throw new IllegalStateException("Partition #" + partitionNumber + " has already been assigned.");
		}

		partitions[partitionNumber] = partition;
		partitionLookupHelper.put(partition.getPartitionId(), partitionNumber);//映射
		partitionsAssigned++;
	}

	public IntermediateDataSetID getId() {
		return id;
	}

	public ExecutionJobVertex getProducer() {
		return producer;
	}

	public IntermediateResultPartition[] getPartitions() {
		return partitions;
	}

	/**
	 * Returns the partition with the given ID.
	 *
	 * @param resultPartitionId ID of the partition to look up
	 * @throws NullPointerException If partition ID <code>null</code>
	 * @throws IllegalArgumentException Thrown if unknown partition ID
	 * @return Intermediate result partition with the given ID
	 *
	 * 通过partition的唯一ID,找到对应的partition的子结果对象
	 * 主要是partition结果用数组存储的，而不是Map,因此需要额外维护这个关系
	 */
	public IntermediateResultPartition getPartitionById(IntermediateResultPartitionID resultPartitionId) {
		// Looks ups the partition number via the helper map and returns the
		// partition. Currently, this happens infrequently enough that we could
		// consider removing the map and scanning the partitions on every lookup.
		// The lookup (currently) only happen when the producer of an intermediate
		// result cannot be found via its registered execution.
		Integer partitionNumber = partitionLookupHelper.get(checkNotNull(resultPartitionId, "IntermediateResultPartitionID"));
		if (partitionNumber != null) {
			return partitions[partitionNumber];
		} else {
			throw new IllegalArgumentException("Unknown intermediate result partition ID " + resultPartitionId);
		}
	}

	public int getNumberOfAssignedPartitions() {
		return partitionsAssigned;
	}

	public ResultPartitionType getResultType() {
		return resultType;
	}

	public int registerConsumer() {
		final int index = numConsumers;
		numConsumers++;

		for (IntermediateResultPartition p : partitions) {
			if (p.addConsumerGroup() != index) {
				throw new RuntimeException("Inconsistent consumer mapping between intermediate result partitions.");
			}
		}
		return index;
	}

	public int getConnectionIndex() {
		return connectionIndex;
	}

	void resetForNewExecution() {
		this.numberOfRunningProducers.set(numParallelProducers);
	}

	int decrementNumberOfRunningProducersAndGetRemaining() {
		return numberOfRunningProducers.decrementAndGet();
	}

	boolean isConsumable() {
		if (resultType.isPipelined()) {
			return true;
		}
		else {
			return numberOfRunningProducers.get() == 0;
		}
	}

	@Override
	public String toString() {
		return "IntermediateResult " + id.toString();
	}
}
