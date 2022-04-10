/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import java.io.IOException;
import java.io.Serializable;

/**
 * Handle to state that can be read back again via {@link #retrieveState()}.
 * 反序列化对象
 */
public interface RetrievableStateHandle<T extends Serializable> extends StateObject {

	/**
	 * Retrieves the object that was previously written to state.
	 * 反序列化成T对象,至于如何反序列化,从hdfs上还是从其他数据源读取数据流返序列化成T,由方法实现
	 */
	T retrieveState() throws IOException, ClassNotFoundException;
}
