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

package org.apache.flink.runtime.io.network.buffer;

import org.apache.flink.core.memory.MemorySegment;

/**
 * A simple buffer recycler that frees the memory segments.
 * 最简单粗暴的回收策略
 */
public class FreeingBufferRecycler implements BufferRecycler {
	
	public static final BufferRecycler INSTANCE = new FreeingBufferRecycler();
	
	// ------------------------------------------------------------------------
	
	// Not instantiable
	private FreeingBufferRecycler() {}

	/**
	 * Frees the given memory segment.
	 * @param memorySegment The memory segment to be recycled.
	 * 直接释放该资源,不进行缓存回收再利用
	 */
	@Override
	public void recycle(MemorySegment memorySegment) {
		memorySegment.free();
	}
}
