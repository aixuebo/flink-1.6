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

package org.apache.flink.runtime.executiongraph.failover;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.util.StringUtils;

import org.slf4j.Logger;

import javax.annotation.Nullable;

/**
 * A utility class to load failover strategies from the configuration.
 * 策略工厂
 */
public class FailoverStrategyLoader {

	/** Config name for the {@link RestartAllStrategy} */
	public static final String FULL_RESTART_STRATEGY_NAME = "full";//默认策略

	/** Config name for the {@link RestartIndividualStrategy} */
	public static final String INDIVIDUAL_RESTART_STRATEGY_NAME = "individual";

	/** Config name for the {@link RestartPipelinedRegionStrategy} */
	public static final String PIPELINED_REGION_RESTART_STRATEGY_NAME = "region";

	// ------------------------------------------------------------------------

	/**
	 * Loads a FailoverStrategy Factory from the given configuration.
	 * 加载配置文件,创建对应的策略实例
	 */
	public static FailoverStrategy.Factory loadFailoverStrategy(Configuration config, @Nullable Logger logger) {
		final String strategyParam = config.getString(JobManagerOptions.EXECUTION_FAILOVER_STRATEGY);

		if (StringUtils.isNullOrWhitespaceOnly(strategyParam)) {//true表示参数是null或者空,说明没有设置策略
			if (logger != null) {
				logger.warn("Null config value for {} ; using default failover strategy (full restarts).",
						JobManagerOptions.EXECUTION_FAILOVER_STRATEGY.key());
			}

			return new RestartAllStrategy.Factory();//走默认策略
		}
		else {
			switch (strategyParam.toLowerCase()) {
				case FULL_RESTART_STRATEGY_NAME:
					return new RestartAllStrategy.Factory();

				case PIPELINED_REGION_RESTART_STRATEGY_NAME:
					return new RestartPipelinedRegionStrategy.Factory();

				case INDIVIDUAL_RESTART_STRATEGY_NAME:
					return new RestartIndividualStrategy.Factory();

				default:
					// we could interpret the parameter as a factory class name and instantiate that
					// for now we simply do not support this
					throw new IllegalConfigurationException("Unknown failover strategy: " + strategyParam);
			}
		}
	}
}
