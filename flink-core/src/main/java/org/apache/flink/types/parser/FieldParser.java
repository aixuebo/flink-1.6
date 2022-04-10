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
import org.apache.flink.types.BooleanValue;
import org.apache.flink.types.ByteValue;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.FloatValue;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.ShortValue;
import org.apache.flink.types.StringValue;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * A FieldParser is used parse a field from a sequence of bytes. Fields occur in a byte sequence and are terminated
 * by the end of the byte sequence or a delimiter.
 * <p>
 * The parsers do not throw exceptions in general, but set an error state. That way, they can be used in functions
 * that ignore invalid lines, rather than failing on them.
 *
 * @param <T> The type that is parsed.
 * 描述了从字节数组中如何解析成java基础属性，并且如果解析失败,会提示失败原因
 * 参见 IntParser 具体的分析
 */
@PublicEvolving
public abstract class FieldParser<T> {
	
	/**
	 * An enumeration of different types of errors that may occur.
	 * 定义错误类型
	 */
	public static enum ParseErrorState {
		/** No error occurred. 没有错误发生*/
		NONE,

		/** The domain of the numeric type is not large enough to hold the parsed value. 数字类型超出范围,比如int是4个字节,结果超出了max和min的范围*/
		NUMERIC_VALUE_OVERFLOW_UNDERFLOW,

		/** A stand-alone sign was encountered while parsing a numeric type. 当解析数字的时候,只发现一个负号,没有其他内容了*/
		NUMERIC_VALUE_ORPHAN_SIGN,

		/** An illegal character was encountered while parsing a numeric type.说明不是数字类型,在解析数字类型时，遇到非数字类型的字节数组,是有问题的 */
		NUMERIC_VALUE_ILLEGAL_CHARACTER,

		/** The field was not in a correct format for the numeric type. 数字类型格式错误*/
		NUMERIC_VALUE_FORMAT_ERROR,

		/** A quoted string was not terminated until the line end. */
		UNTERMINATED_QUOTED_STRING,

		/** The parser found characters between the end of the quoted string and the delimiter. */
		UNQUOTED_CHARS_AFTER_QUOTED_STRING,

		/** The column is empty. 这个属性值为""*/
		EMPTY_COLUMN,

		/** Invalid Boolean value boolean类型数据解析失败**/
		BOOLEAN_INVALID
	}

	private Charset charset = StandardCharsets.UTF_8;

	private ParseErrorState errorState = ParseErrorState.NONE;//默认无错误

	/**
	 * Parses the value of a field from the byte array, taking care of properly reset
	 * the state of this parser.
	 * The start position within the byte array and the array's valid length is given.
	 * The content of the value is delimited by a field delimiter.
	 * 
	 * @param bytes The byte array that holds the value.待解析的直接数组
	 * @param startPos The index where the field starts 从字节数组什么位置开始解析
	 * @param limit The limit unto which the byte contents is valid for the parser. The limit is the
	 *              position one after the last valid byte.需要从字节数组中读取多少个字节
	 * @param delim The field delimiter character 结束符
	 * @param reuse An optional reusable field to hold the value 解析后的返回值
	 * 
	 * @return The index of the next delimiter, if the field was parsed correctly. A value less than 0 otherwise.
	 */
	public int resetErrorStateAndParse(byte[] bytes, int startPos, int limit, byte[] delim, T reuse) {
		resetParserState();//重置状态
		return parseField(bytes, startPos, limit, delim, reuse);
	}

	/**
	 * Each parser's logic should be implemented inside this method 从字节数组中解析对象 -- 相当于反序列化的过程,但有错误提示,为什么会解析失败
	 * @param bytes 待被解析的字节数组
	 * @param startPos 从这个位置开始解析
	 * @param limit 读取的字节长度
	 * @param delim 分隔符内容
	 * @param reuse 解析后的数据填充到该数据内
	 * @return -1 表示遇到解释失败情况 假设返回100,说明读取了100个字节数组,理论上返回的应该等同于limit
	 */
	protected abstract int parseField(byte[] bytes, int startPos, int limit, byte[] delim, T reuse);

	/**
	 * Reset the state of the parser. Called as the very first method inside
	 * {@link FieldParser#resetErrorStateAndParse(byte[], int, int, byte[], Object)}, by default it just reset
	 * its error state.
	 * */
	protected void resetParserState() {
		this.errorState = ParseErrorState.NONE;
	}

	/**
	 * Gets the parsed field. This method returns the value parsed by the last successful invocation of
	 * {@link #parseField(byte[], int, int, byte[], Object)}. It objects are mutable and reused, it will return
	 * the object instance that was passed the parse function.
	 * 
	 * @return The latest parsed field.
	 * 获取解析后的具体的值
	 */
	public abstract T getLastResult();
	
	/**
	 * Returns an instance of the parsed value type.
	 * 
	 * @return An instance of the parsed value type.
	 * 初始化一个值
	 */
	public abstract T createValue();
	
	/**
	 * Checks if the delimiter starts at the given start position of the byte array.
	 * 
	 * Attention: This method assumes that enough characters follow the start position for the delimiter check!
	 * 
	 * @param bytes The byte array that holds the value.
	 * @param startPos The index of the byte array where the check for the delimiter starts.
	 * @param delim The delimiter to check for.
	 * 
	 * @return true if a delimiter starts at the given start position, false otherwise.
	 * true 表示bytes字节内容从startPos开始,读取delim.length长度后,与delim的内容相同。
	 * 即接下来都是分隔符
	 */
	public static final boolean delimiterNext(byte[] bytes, int startPos, byte[] delim) {

		for(int pos = 0; pos < delim.length; pos++) {
			// check each position
			if(delim[pos] != bytes[startPos+pos]) {
				return false;
			}
		}
		return true;
		
	}

	/**
	 * Checks if the given bytes ends with the delimiter at the given end position.
	 *
	 * @param bytes  The byte array that holds the value.
	 * @param endPos The index of the byte array where the check for the delimiter ends.
	 * @param delim  The delimiter to check for.
	 *
	 * @return true if a delimiter ends at the given end position, false otherwise.
	 * true表示 bytes字节数组从endpos之后,所有的数据内容,正好是delim分隔符
	 */
	public static final boolean endsWithDelimiter(byte[] bytes, int endPos, byte[] delim) {
		if (endPos < delim.length - 1) {
			return false;
		}
		for (int pos = 0; pos < delim.length; ++pos) {
			if (delim[pos] != bytes[endPos - delim.length + 1 + pos]) {
				return false;
			}
		}
		return true;
	}
	
	/**
	 * Sets the error state of the parser. Called by subclasses of the parser to set the type of error
	 * when failing a parse.
	 * 
	 * @param error The error state to set.
	 */
	protected void setErrorState(ParseErrorState error) {
		this.errorState = error;
	}
	
	/**
	 * Gets the error state of the parser, as a value of the enumeration {@link ParseErrorState}.
	 * If no error occurred, the error state will be {@link ParseErrorState#NONE}.
	 * 
	 * @return The current error state of the parser.
	 * 获取错误状态
	 */
	public ParseErrorState getErrorState() {
		return this.errorState;
	}

	/**
	 * Returns the end position of a string. Sets the error state if the column is empty.
	 *
	 * @return the end position of the string or -1 if an error occurred
	 * 返回分隔符前的位置,即从startPos到返回值,就是需要解析值需要的字节数组
	 */
	protected final int nextStringEndPos(byte[] bytes, int startPos, int limit, byte[] delimiter) {
		int endPos = startPos;//定义分隔符前的位置

		final int delimLimit = limit - delimiter.length + 1;//分隔符字节的开始位置

		while (endPos < limit) {
			if (endPos < delimLimit && delimiterNext(bytes, endPos, delimiter)) {//遇到下一个分隔符,退出循环,找到endPos
				break;
			}
			endPos++;
		}

		if (endPos == startPos) {
			setErrorState(ParseErrorState.EMPTY_COLUMN);//说明是空字符串
			return -1;
		}
		return endPos;
	}

	/**
	 * Returns the length of a string. Throws an exception if the column is empty.
	 *
	 * @return the length of the string
	 * 返回分隔符前的位置,即从startPos到返回值,就是需要解析值需要的字节数组
	 */
	protected static final int nextStringLength(byte[] bytes, int startPos, int length, char delimiter) {
		if (length <= 0) {
			throw new IllegalArgumentException("Invalid input: Empty string");
		}
		int limitedLength = 0;
		final byte delByte = (byte) delimiter;

		while (limitedLength < length && bytes[startPos + limitedLength] != delByte) {
			limitedLength++;
		}

		return limitedLength;
	}

	/**
	 * Gets the character set used for this parser.
	 *
	 * @return the charset used for this parser.
	 */
	public Charset getCharset() {
		return this.charset;
	}

	/**
	 * Sets the character set used for this parser.
	 *
	 * @param charset charset used for this parser.
	 */
	public void setCharset(Charset charset) {
		this.charset = charset;
	}

	// --------------------------------------------------------------------------------------------
	//  Mapping from types to parsers
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Gets the parser for the type specified by the given class. Returns null, if no parser for that class
	 * is known.
	 * 
	 * @param type The class of the type to get the parser for.
	 * @return The parser for the given type, or null, if no such parser exists.
	 * 给定java基础类型,返回如何解析基础类型的对象，相当于工厂类
	 */
	public static <T> Class<FieldParser<T>> getParserForType(Class<T> type) {
		Class<? extends FieldParser<?>> parser = PARSERS.get(type);
		if (parser == null) {
			return null;
		} else {
			@SuppressWarnings("unchecked")
			Class<FieldParser<T>> typedParser = (Class<FieldParser<T>>) parser;
			return typedParser;
		}
	}

	//从字节数组中如何解析对象
	private static final Map<Class<?>, Class<? extends FieldParser<?>>> PARSERS = 
			new HashMap<Class<?>, Class<? extends FieldParser<?>>>();
	
	static {
		// basic types
		PARSERS.put(Byte.class, ByteParser.class);
		PARSERS.put(Short.class, ShortParser.class);
		PARSERS.put(Integer.class, IntParser.class);
		PARSERS.put(Long.class, LongParser.class);
		PARSERS.put(String.class, StringParser.class);
		PARSERS.put(Float.class, FloatParser.class);
		PARSERS.put(Double.class, DoubleParser.class);
		PARSERS.put(Boolean.class, BooleanParser.class);
		PARSERS.put(BigDecimal.class, BigDecParser.class);
		PARSERS.put(BigInteger.class, BigIntParser.class);

		// value types
		PARSERS.put(ByteValue.class, ByteValueParser.class);
		PARSERS.put(ShortValue.class, ShortValueParser.class);
		PARSERS.put(IntValue.class, IntValueParser.class);
		PARSERS.put(LongValue.class, LongValueParser.class);
		PARSERS.put(StringValue.class, StringValueParser.class);
		PARSERS.put(FloatValue.class, FloatValueParser.class);
		PARSERS.put(DoubleValue.class, DoubleValueParser.class);
		PARSERS.put(BooleanValue.class, BooleanValueParser.class);

		// SQL date/time types
		PARSERS.put(java.sql.Time.class, SqlTimeParser.class);
		PARSERS.put(java.sql.Date.class, SqlDateParser.class);
		PARSERS.put(java.sql.Timestamp.class, SqlTimestampParser.class);
	}
}
