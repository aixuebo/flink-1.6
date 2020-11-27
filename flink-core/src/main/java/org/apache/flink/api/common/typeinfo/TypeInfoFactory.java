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

package org.apache.flink.api.common.typeinfo;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import java.lang.reflect.Type;
import java.util.Map;

/**
 * Base class for implementing a type information factory. A type information factory allows for
 * plugging-in user-defined {@link TypeInformation} into the Flink type system. The factory is
 * called during the type extraction phase if the corresponding type has been annotated with
 * {@link TypeInfo}. In a hierarchy of types the closest factory will be chosen while traversing
 * upwards, however, a globally registered factory has highest precedence
 * (see {@link TypeExtractor#registerFactory(Type, Class)}).
 *
 * flink提供了基础对象的序列化等转换形式,但如果开发者自己定义一个对象,比如User,也要让flink支持,则需要自己为User创建一个插件,支持排序比较、序列化
 * @param <T> type for which {@link TypeInformation} is created,我们要把原始的T对象创建成一个flink的对象
 */
@Public
public abstract class TypeInfoFactory<T> {

	/**
	 * Creates type information for the type the factory is targeted for. The parameters provide
	 * additional information about the type itself as well as the type's generic type parameters.
	 * 用于创建对象的类型,参数提供了附加的信息
	 *
	 * @param t the exact type the type information is created for; might also be a subclass of &lt;T&gt; 是要包装的对象,比如User 或者User的子类,User就表示泛型T,即为泛型T对象创建flink对象
	 * @param genericParameters mapping of the type's generic type parameters to type information
	 *                          extracted with Flink's type extraction facilities; null values
	 *                          indicate that type information could not be extracted for this parameter
	 *                          创建对象时，需要哪些基础元素
	 * @return type information for the type the factory is targeted for
	 *
	 * 如何根据参数创建一个类型
	 */
	public abstract TypeInformation<T> createTypeInfo(Type t, Map<String, TypeInformation<?>> genericParameters);

}
