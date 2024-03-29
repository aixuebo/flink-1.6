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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;

import java.io.IOException;

/**
 * An abstract base implementation of the {@link StateBackend} interface.
 *
 * <p>This class has currently no contents and only kept to not break the prior class hierarchy for users.
 */
@PublicEvolving
public abstract class AbstractStateBackend implements StateBackend, java.io.Serializable {

	private static final long serialVersionUID = 4620415814639230247L;

	// ------------------------------------------------------------------------
	//  State Backend - State-Holding Backends
	// ------------------------------------------------------------------------

	//创建存储key的对象
	@Override
	public abstract <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
		Environment env,
		JobID jobID,
		String operatorIdentifier,
		TypeSerializer<K> keySerializer,
		int numberOfKeyGroups,
		KeyGroupRange keyGroupRange,
		TaskKvStateRegistry kvStateRegistry,
		TtlTimeProvider ttlTimeProvider) throws IOException;

	//创建正常的存储对象
	@Override
	public abstract OperatorStateBackend createOperatorStateBackend(
			Environment env,
			String operatorIdentifier) throws Exception;
}
