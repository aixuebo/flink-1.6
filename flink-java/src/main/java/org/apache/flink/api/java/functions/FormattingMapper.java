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

package org.apache.flink.api.java.functions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.io.TextOutputFormat.TextFormatter;

/**
 * Mapper that converts values to strings using a {@link TextFormatter}.
 * @param <T>
 * 将每一个元素，进行格式化,转换成字符串形式
 */
@Internal
public class FormattingMapper<T> implements MapFunction<T, String> {
	private static final long serialVersionUID = 1L;

	private final TextFormatter<T> formatter;

	public FormattingMapper(TextOutputFormat.TextFormatter<T> formatter) {
		this.formatter = formatter;
	}

	@Override
	public String map(T value) throws Exception {
		return formatter.format(value);
	}
}
