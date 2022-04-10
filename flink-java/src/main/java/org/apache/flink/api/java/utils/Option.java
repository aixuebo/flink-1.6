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

package org.apache.flink.api.java.utils;

import org.apache.flink.annotation.PublicEvolving;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Internal representation of a parameter passed to a user defined function.
 * 代表一个参数
 */
@PublicEvolving
public class Option {

	private String longName;//参数长名称
	private String shortName;//参数短名称

	private String defaultValue;//参数默认值
	private Set<String> choices;//该参数有效的值 --- 比如参数值只允许是0和1,此时就需要将所有的有效值赋予给参数

	private String helpText;//参数帮助提示信息
	private OptionType type = OptionType.STRING;//参数类型,默认都是String类型

	public Option(String name) {
		this.longName = name;
		this.choices = new HashSet<>();
	}

	/**
	 * Define an alternative / short name of the parameter.
	 * Only one alternative per parameter is allowed.
	 *
	 * @param shortName - short version of the parameter name
	 * @return the updated Option
	 */
	public Option alt(String shortName) {
		this.shortName = shortName;
		return this;
	}

	/**
	 * Define the type of the Option.
	 *
	 * @param type - the type which the value of the Option can be casted to.
	 * @return the updated Option
	 */
	public Option type(OptionType type) {
		this.type = type;
		return this;
	}

	/**
	 * Define a default value for the option.
	 *
	 * @param defaultValue - the default value
	 * @return the updated Option
	 * @throws RequiredParametersException if the list of possible values for the parameter is not empty and the default
	 *                                     value passed is not in the list.
	 */
	public Option defaultValue(String defaultValue) throws RequiredParametersException {
		if (this.choices.isEmpty()) {
			return this.setDefaultValue(defaultValue);
		} else {
			if (this.choices.contains(defaultValue)) {//默认值是有效的
				return this.setDefaultValue(defaultValue);
			} else {
				throw new RequiredParametersException("Default value " + defaultValue +
						" is not in the list of valid values for option " + this.longName);
			}
		}
	}

	/**
	 * Restrict the list of possible values of the parameter.
	 *
	 * @param choices - the allowed values of the parameter.
	 * @return the updated Option
	 * 添加参数的有效值集合
	 */
	public Option choices(String... choices) throws RequiredParametersException {
		if (this.defaultValue != null) {
			if (Arrays.asList(choices).contains(defaultValue)) {//默认值必须包含在有效值内
				Collections.addAll(this.choices, choices);
			} else {//说明默认值不在有效值内,则抛异常
				throw new RequiredParametersException("Valid values for option " + this.longName +
						" do not contain defined default value " + defaultValue);
			}
		} else {//直接设置参数有效值集合
			Collections.addAll(this.choices, choices);
		}
		return this;
	}

	/**
	 * Add a help text, explaining the parameter.
	 *
	 * @param helpText - the help text.
	 * @return the updated Option
	 */
	public Option help(String helpText) {
		this.helpText = helpText;
		return this;
	}

	public String getName() {
		return this.longName;
	}

	//是否有短名称
	public boolean hasAlt() {
		return this.shortName != null;
	}

	//是否设置了参数类型
	public boolean hasType() {
		return this.type != null;
	}

	public OptionType getType() {
		return this.type;
	}

	public String getAlt() {
		return this.shortName;
	}

	public String getHelpText() {
		return this.helpText;
	}

	public Set<String> getChoices() {
		return this.choices;
	}

	//是否有默认值
	public boolean hasDefaultValue() {
		return this.defaultValue != null;
	}

	public String getDefaultValue() {
		return this.defaultValue;
	}

	private Option setDefaultValue(String defaultValue) {
		this.defaultValue = defaultValue;
		return this;
	}

	//是否value可以转换成参数需要的类型值
	public boolean isCastableToDefinedType(String value) {
		switch (this.type) {
			case INTEGER:
				try {
					Integer.parseInt(value);
				} catch (NumberFormatException nfe) {
					return false;
				}
				return true;
			case LONG:
				try {
					Long.parseLong(value);
				} catch (NumberFormatException nfe) {
					return false;
				}
				return true;
			case FLOAT:
				try {
					Float.parseFloat(value);
				} catch (NumberFormatException nfe) {
					return false;
				}
				return true;
			case DOUBLE:
				try {
					Double.parseDouble(value);
				} catch (NumberFormatException nfe) {
					return false;
				}
				return true;
			case BOOLEAN:
				return Objects.equals(value, "true") || Objects.equals(value, "false");
			case STRING:
				return true;
		}
		throw new IllegalStateException("Invalid value for OptionType " + this.type + " for option " + this.longName);
	}

}
