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

package org.apache.flink.api.common.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.operators.util.FieldList;
import org.apache.flink.api.common.operators.util.FieldSet;

import java.util.ArrayList;

/**
 * This class represents an ordering on a set of fields. It specifies the fields and order direction
 * (ascending, descending).
 *
 比如sql:
 select id,name,age
 from xxx
 order by name,age

 则indexes内容是2，3
 types内容是string，int
 orders内容是 desc desc

 标识好整体如何排序的
 */
@Internal
public class Ordering implements Cloneable {

	//存储要排序的id序号集合,是有顺序的
	protected FieldList indexes = new FieldList();
	
	protected final ArrayList<Class<? extends Comparable<?>>> types = new ArrayList<Class<? extends Comparable<?>>>();//每一个index的值如何比较
	
	protected final ArrayList<Order> orders = new ArrayList<Order>(); //每一个index是倒序还是正序,即排序方式

	// --------------------------------------------------------------------------------------------
	
	/**
	 * Creates an empty ordering.
	 */
	public Ordering() {}
	
	/**
	 * 按照一个字段排序
	 * @param index
	 * @param type
	 * @param order
	 */
	public Ordering(int index, Class<? extends Comparable<?>> type, Order order) {
		appendOrdering(index, type, order);
	}
	
	/**
	 * Extends this ordering by appending an additional order requirement.
	 * If the index has been previously appended then the unmodified Ordering
	 * is returned.
	 * 
	 * @param index Field index of the appended order requirement.第几个序号
	 * @param type Type of the appended order requirement. 如何比较
	 * @param order Order of the appended order requirement. 倒序还是正序,即排序方式
	 * 
	 * @return This ordering with an additional appended order requirement.
	 * 追加一个排序字段
	 */
	public Ordering appendOrdering(Integer index, Class<? extends Comparable<?>> type, Order order) {
		if (index < 0) {
			throw new IllegalArgumentException("The key index must not be negative.");
		}
		if (order == null) {
			throw new NullPointerException();
		}
		if (order == Order.NONE) {
			throw new IllegalArgumentException("An ordering must not be created with a NONE order.");
		}

		if (!this.indexes.contains(index)) {
			this.indexes = this.indexes.addField(index);
			this.types.add(type);
			this.orders.add(order);
		}

		return this;
	}
	
	// --------------------------------------------------------------------------------------------
	//多少个排序的字段
	public int getNumberOfFields() {
		return this.indexes.size();
	}

	//哪几个序号是用来order by的字段
	public FieldList getInvolvedIndexes() {
		return this.indexes;
	}

	//获取第几个排序的字段具体对应的排序索引序号
	public Integer getFieldNumber(int index) {
		if (index < 0 || index >= this.indexes.size()) {
			throw new IndexOutOfBoundsException(String.valueOf(index));
		}
		return this.indexes.get(index);
	}

	//获取该字段如何排序
	public Class<? extends Comparable<?>> getType(int index) {
		if (index < 0 || index >= this.types.size()) {
			throw new IndexOutOfBoundsException(String.valueOf(index));
		}
		return this.types.get(index);
	}

	//获取该字段要正序还是倒序
	public Order getOrder(int index) {
		if (index < 0 || index >= this.types.size()) {
			throw new IndexOutOfBoundsException(String.valueOf(index));
		}
		return orders.get(index);
	}
	
	// --------------------------------------------------------------------------------------------
	//返回每一个元素的比较器
	@SuppressWarnings("unchecked")
	public Class<? extends Comparable<?>>[] getTypes() {
		return this.types.toArray(new Class[this.types.size()]);
	}

	//返回哪些字段需要被排序
	public int[] getFieldPositions() {
		final int[] ia = new int[this.indexes.size()];
		for (int i = 0; i < ia.length; i++) {
			ia[i] = this.indexes.get(i);
		}
		return ia;
	}

	//返回每一个字段排序的方式--正序/倒序
	public Order[] getFieldOrders() {
		return this.orders.toArray(new Order[this.orders.size()]);
	}	

	//每一个字段是否倒序 -- true表示倒序
	public boolean[] getFieldSortDirections() {
		final boolean[] directions = new boolean[this.orders.size()];
		for (int i = 0; i < directions.length; i++) {
			directions[i] = this.orders.get(i) != Order.DESCENDING; 
		}
		return directions;
	}
	
	// --------------------------------------------------------------------------------------------
	
	public boolean isMetBy(Ordering otherOrdering) {
		if (otherOrdering == null || this.indexes.size() > otherOrdering.indexes.size()) {
			return false;
		}
		
		for (int i = 0; i < this.indexes.size(); i++) {
			if (!this.indexes.get(i).equals(otherOrdering.indexes.get(i))) {
				return false;
			}
			
			// if this one request no order, everything is good
			if (this.orders.get(i) != Order.NONE) {
				if (this.orders.get(i) == Order.ANY) {
					// if any order is requested, any not NONE order is good
					if (otherOrdering.orders.get(i) == Order.NONE) {
						return false;
					}
				} else if (otherOrdering.orders.get(i) != this.orders.get(i)) {
					// the orders must be equal
					return false;
				}
			}
		}
		return true;
	}
	
	public boolean isOrderEqualOnFirstNFields(Ordering other, int n) {
		if (n > getNumberOfFields() || n > other.getNumberOfFields()) {
			throw new IndexOutOfBoundsException();
		}
		
		for (int i = 0; i < n; i++) {
			final Order o = this.orders.get(i);
			if (o == Order.NONE || o == Order.ANY || o != other.orders.get(i)) {
				return false;
			}
		}
		
		return true;
	}
	
	/**
	 * Creates a new ordering the represents an ordering on a prefix of the fields. If the
	 * exclusive index up to which to create the ordering is <code>0</code>, then there is
	 * no resulting ordering and this method return <code>null</code>.
	 * 
	 * @param exclusiveIndex The index (exclusive) up to which to create the ordering.
	 * @return The new ordering on the prefix of the fields, or <code>null</code>, if the prefix is empty.
	 */
	public Ordering createNewOrderingUpToIndex(int exclusiveIndex) {
		if (exclusiveIndex == 0) {
			return null;
		}
		final Ordering newOrdering = new Ordering();
		for (int i = 0; i < exclusiveIndex; i++) {
			newOrdering.appendOrdering(this.indexes.get(i), this.types.get(i), this.orders.get(i));
		}
		return newOrdering;
	}
	
	public boolean groupsFields(FieldSet fields) {
		if (fields.size() > this.indexes.size()) {
			return false;
		}
		
		for (int i = 0; i < fields.size(); i++) {
			if (!fields.contains(this.indexes.get(i))) {
				return false;
			}
		}
		return true;
	}
	
	// --------------------------------------------------------------------------------------------

	public Ordering clone() {
		Ordering newOrdering = new Ordering();
		newOrdering.indexes = this.indexes;
		newOrdering.types.addAll(this.types);
		newOrdering.orders.addAll(this.orders);
		return newOrdering;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((indexes == null) ? 0 : indexes.hashCode());
		result = prime * result + ((orders == null) ? 0 : orders.hashCode());
		result = prime * result + ((types == null) ? 0 : types.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		Ordering other = (Ordering) obj;
		if (indexes == null) {
			if (other.indexes != null) {
				return false;
			}
		} else if (!indexes.equals(other.indexes)) {
			return false;
		}
		if (orders == null) {
			if (other.orders != null) {
				return false;
			}
		} else if (!orders.equals(other.orders)) {
			return false;
		}
		if (types == null) {
			if (other.types != null) {
				return false;
			}
		} else if (!types.equals(other.types)) {
			return false;
		}
		return true;
	}

	public String toString() {
		final StringBuilder buf = new StringBuilder("[");
		for (int i = 0; i < indexes.size(); i++) {
			if (i != 0) {
				buf.append(",");
			}
			buf.append(this.indexes.get(i));
			if (this.types.get(i) != null) {
				buf.append(":");
				buf.append(this.types.get(i).getName());
			}
			buf.append(":");
			buf.append(this.orders.get(i).getShortName());
		}
		buf.append("]");
		return buf.toString();
	}
}
