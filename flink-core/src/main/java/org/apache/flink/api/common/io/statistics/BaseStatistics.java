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


package org.apache.flink.api.common.io.statistics;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.Public;

/**
 * Interface describing the basic statistics that can be obtained from the input.
 * 统计数量源大小、有多少条数据、每一条数据平均字节数
 */
@Public
public interface BaseStatistics {
	
	/**
	 * Constant indicating that the input size is unknown.
	 * 不清楚文件大小
	 */
	@PublicEvolving
	public static final long SIZE_UNKNOWN = -1;
	
	/**
	 * Constant indicating that the number of records is unknown;
	 * 不清楚有多少条数据
	 */
	@PublicEvolving
	public static final long NUM_RECORDS_UNKNOWN = -1;
	
	/**
	 * Constant indicating that average record width is unknown.
	 * 不清楚平均每一个数据的长度
	 */
	@PublicEvolving
	public static final float AVG_RECORD_BYTES_UNKNOWN = -1.0f;
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Gets the total size of the input.
	 *   
	 * @return The total size of the input, in bytes.
	 * 获取文件总大小
	 */
	@PublicEvolving
	public long getTotalInputSize();
	
	/**
	 * Gets the number of records in the input (= base cardinality).
	 * 
	 * @return The number of records in the input.
	 * 文件总行数
	 */
	@PublicEvolving
	public long getNumberOfRecords();
	
	/**
	 * Gets the average width of a record, in bytes.
	 * 
	 * @return The average width of a record in bytes.
	 * 文件数据平均长度
	 */
	@PublicEvolving
	public float getAverageRecordWidth();
}
