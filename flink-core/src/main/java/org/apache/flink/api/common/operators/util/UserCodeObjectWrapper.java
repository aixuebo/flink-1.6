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

package org.apache.flink.api.common.operators.util;

import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.NonSerializableUserCodeException;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This holds an actual object containing user defined code.
 */
@Internal
public class UserCodeObjectWrapper<T> implements UserCodeWrapper<T> {
	private static final long serialVersionUID = 1L;
	
	private final T userCodeObject;//T是要支持序列化能力的类
	
	public UserCodeObjectWrapper(T userCodeObject) {
		checkNotNull(userCodeObject, "The user code object may not be null.");
		checkArgument(userCodeObject instanceof Serializable, "User code object is not serializable: " + userCodeObject.getClass().getName());
		
		this.userCodeObject = userCodeObject;

		//主要用于校验，是否所有的属性都能被序列化
		// Remove non serializable objects from the user code object as well as from outer objects
		Object current = userCodeObject;
		try {
			while (null != current) {
				Object newCurrent = null;
				
				/**
				 * Check if the usercode class has custom serialization methods.
				 * (See http://docs.oracle.com/javase/7/docs/api/java/io/Serializable.html for details).
				 * We can not guarantee that the user handles serialization correctly in this case.
				 * 是否自定义了序列化与反序列化方式
				 */
				boolean hasCustomSerialization = false;//true自定义了
				Method customSerializer = null;//如何序列化与反序列化的方法
				Method customDeserializer = null;
				try {
					customSerializer = current.getClass().getDeclaredMethod("writeObject", java.io.ObjectOutputStream.class);
					customDeserializer = current.getClass().getDeclaredMethod("readObject", java.io.ObjectInputStream.class);
				} catch (Exception e) {
					// we can ignore exceptions here.
				}
				
				if (customSerializer != null && customDeserializer != null) {
					hasCustomSerialization = true;
				}

				//序列化属性
				for (Field f : current.getClass().getDeclaredFields()) {
					f.setAccessible(true);

					if (f.getName().contains("$outer")) {
						newCurrent = f.get(current);
					}

					//自定义的不需要序列化属性、静态的不需要序列化属性、Transient不需要序列化属性
					if (hasCustomSerialization || Modifier.isTransient(f.getModifiers()) || Modifier.isStatic(f.getModifiers())) {
						// field not relevant for serialization
						continue;
					}
					
					Object fieldContents = f.get(current);//获取要序列化的属性值
					if (fieldContents != null &&  !(fieldContents instanceof Serializable)) {//确保该值一定是可以被序列化的类型,否则肯定会序列化失败呀
						throw new NonSerializableUserCodeException("User-defined object " + userCodeObject + " (" + 
							userCodeObject.getClass().getName() + ") contains non-serializable field " +
							f.getName() + " = " + f.get(current));
					}
				}
				current = newCurrent;
			}
		}
		catch (NonSerializableUserCodeException e) {
			// forward those
			throw e;
		}
		catch (Exception e) {
			// should never happen, since we make the fields accessible.
			// anyways, do not swallow the exception, but report it
			throw new RuntimeException("Could not access the fields of the user defined class while checking for serializability.", e);
		}
	}

	//返回需要被序列化的实例
	@Override
	public T getUserCodeObject(Class<? super T> superClass, ClassLoader cl) {
		return userCodeObject;
	}
	
	@Override
	public T getUserCodeObject() {
		return userCodeObject;
		
	}

	//获取T的某一个Annotation
	@Override
	public <A extends Annotation> A getUserCodeAnnotation(Class<A> annotationClass) {
		return userCodeObject.getClass().getAnnotation(annotationClass);
	}

	//获取T实例对应的class
	@SuppressWarnings("unchecked")
	@Override
	public Class<? extends T> getUserCodeClass() {
		return (Class<? extends T>) userCodeObject.getClass();
	}
	
	@Override
	public boolean hasObject() {
		return true;
	}
}
