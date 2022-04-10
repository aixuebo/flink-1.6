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


package org.apache.flink.types.parser;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Parses a decimal text field into a IntValue.
 * Only characters '1' to '0' and '-' are allowed.
 * The parser does not check for the maximum value.
 */
@PublicEvolving
public class IntParser extends FieldParser<Integer> {

	private static final long OVERFLOW_BOUND = 0x7fffffffL;
	private static final long UNDERFLOW_BOUND = 0x80000000L;

	private int result;

	//从字节数组中解析对象
	@Override
	public int parseField(byte[] bytes, int startPos, int limit, byte[] delimiter, Integer reusable) {

		if (startPos == limit) {
			setErrorState(ParseErrorState.EMPTY_COLUMN);//这个属性值为""
			return -1;
		}

		long val = 0;
		boolean neg = false;//说明是负数

		final int delimLimit = limit - delimiter.length + 1;//分隔符的开始位置

		if (bytes[startPos] == '-') {
			neg = true;
			startPos++;

			// check for empty field with only the sign
			if (startPos == limit //如果仅有一个负号,没有其他数值
				|| (startPos < delimLimit && delimiterNext(bytes,startPos,delimiter))) {//即接下来都是分隔符
				setErrorState(ParseErrorState.NUMERIC_VALUE_ORPHAN_SIGN);
				return -1;
			}
		}

		for (int i = startPos; i < limit; i++) {
			if (i < delimLimit && delimiterNext(bytes, i, delimiter)) {//说明虽然字节数组bytes是有内容的,但内容都是分隔符delimiter,因此状态是""
				if (i == startPos) {
					setErrorState(ParseErrorState.EMPTY_COLUMN);
					return -1;
				}
				this.result = (int) (neg ? -val : val);//遇到结束分隔符了,根据正负号输出正确结果
				return i + delimiter.length;
			}
			if (bytes[i] < 48 || bytes[i] > 57) {
				setErrorState(ParseErrorState.NUMERIC_VALUE_ILLEGAL_CHARACTER);//说明不是数字类型,在解析数字类型时，遇到非数字类型的字节数组,是有问题的
				return -1;
			}
			val *= 10;
			val += bytes[i] - 48;

			if (val > OVERFLOW_BOUND && (!neg || val > UNDERFLOW_BOUND)) {
				setErrorState(ParseErrorState.NUMERIC_VALUE_OVERFLOW_UNDERFLOW);//数字类型超出范围,比如int是4个字节,结果超出了max和min的范围
				return -1;
			}
		}

		this.result = (int) (neg ? -val : val);
		return limit;
	}

	@Override
	public Integer createValue() {
		return Integer.MIN_VALUE;
	}

	@Override
	public Integer getLastResult() {
		return Integer.valueOf(this.result);
	}

	/**
	 * Static utility to parse a field of type int from a byte sequence that represents text 
	 * characters
	 * (such as when read from a file stream).
	 *
	 * @param bytes    The bytes containing the text data that should be parsed.待被解析的字节数组
	 * @param startPos The offset to start the parsing.开始解析的位置
	 * @param length   The length of the byte sequence (counting from the offset).需要解析的字节长度
	 * @return The parsed value. 解析后的返回值
	 * @throws NumberFormatException Thrown when the value cannot be parsed because the text 
	 * represents not a correct number.
	 */
	public static final int parseField(byte[] bytes, int startPos, int length) {
		return parseField(bytes, startPos, length, (char) 0xffff);
	}

	/**
	 * Static utility to parse a field of type int from a byte sequence that represents text 
	 * characters
	 * (such as when read from a file stream).
	 *
	 * @param bytes     The bytes containing the text data that should be parsed.
	 * @param startPos  The offset to start the parsing.
	 * @param length    The length of the byte sequence (counting from the offset).
	 * @param delimiter The delimiter that terminates the field.
	 * @return The parsed value.
	 * @throws NumberFormatException Thrown when the value cannot be parsed because the text 
	 * represents not a correct number.
	 * 返回一个int
	 */
	public static final int parseField(byte[] bytes, int startPos, int length, char delimiter) {
		long val = 0;
		boolean neg = false;//true表示负数

		if (bytes[startPos] == delimiter) {
			throw new NumberFormatException("Empty field.");
		}

		if (bytes[startPos] == '-') {
			neg = true;
			startPos++;
			length--;
			if (length == 0 || bytes[startPos] == delimiter) { //下一个就是结束符,或者长度没有了,说明只有一个-号
				throw new NumberFormatException("Orphaned minus sign.");
			}
		}

		for (; length > 0; startPos++, length--) {
			if (bytes[startPos] == delimiter) {
				return (int) (neg ? -val : val);//负负为正
			}
			if (bytes[startPos] < 48 || bytes[startPos] > 57) {//说明不是int类型
				throw new NumberFormatException("Invalid character.");
			}
			val *= 10;
			val += bytes[startPos] - 48;

			if (val > OVERFLOW_BOUND && (!neg || val > UNDERFLOW_BOUND)) {
				throw new NumberFormatException("Value overflow/underflow");
			}
		}
		return (int) (neg ? -val : val);//该负就负
	}
}
