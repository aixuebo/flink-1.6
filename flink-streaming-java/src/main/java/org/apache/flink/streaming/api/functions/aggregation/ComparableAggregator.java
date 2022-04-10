/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions.aggregation;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.util.typeutils.FieldAccessor;
import org.apache.flink.streaming.util.typeutils.FieldAccessorFactory;

/**
 * An {@link AggregationFunction} that computes values based on comparisons of
 * {@link Comparable Comparables}.
 */
@Internal
public class ComparableAggregator<T> extends AggregationFunction<T> {

	private static final long serialVersionUID = 1L;

	private Comparator comparator;
	private boolean byAggregate;//是否minBy操作,即是最小值，还是保留最小的记录
	private boolean first;//当出现相同的元素时，排序顺序保留的是第几次出现的内容,是第一次，还是最后一次
	private final FieldAccessor<T, Object> fieldAccessor;

	private ComparableAggregator(AggregationType aggregationType, FieldAccessor<T, Object> fieldAccessor, boolean first) {
		this.comparator = Comparator.getForAggregation(aggregationType);
		this.byAggregate = (aggregationType == AggregationType.MAXBY) || (aggregationType == AggregationType.MINBY);
		this.first = first;
		this.fieldAccessor = fieldAccessor;
	}

	public ComparableAggregator(int positionToAggregate,
			TypeInformation<T> typeInfo,
			AggregationType aggregationType,
			ExecutionConfig config) {
		this(positionToAggregate, typeInfo, aggregationType, false, config);
	}

	public ComparableAggregator(int positionToAggregate,
			TypeInformation<T> typeInfo,
			AggregationType aggregationType,
			boolean first,
			ExecutionConfig config) {
		this(aggregationType, FieldAccessorFactory.getAccessor(typeInfo, positionToAggregate, config), first);
	}

	public ComparableAggregator(String field,
			TypeInformation<T> typeInfo,
			AggregationType aggregationType,
			boolean first,
			ExecutionConfig config) {
		this(aggregationType, FieldAccessorFactory.getAccessor(typeInfo, field, config), first);
	}

	@SuppressWarnings("unchecked")
	@Override
	public T reduce(T value1, T value2) throws Exception {
		Comparable<Object> o1 = (Comparable<Object>) fieldAccessor.get(value1);
		Object o2 = fieldAccessor.get(value2);

		int c = comparator.isExtremal(o1, o2);

		if (byAggregate) {
			// if they are the same we choose based on whether we want to first or last
			// element with the min/max.
			if (c == 0) { //byAggregate时,c=0表示遇到相同的数据
				return first ? value1 : value2; //直接返回value1或者value2整条数据
			}

			return c == 1 ? value1 : value2;//直接返回value1或者value2整条数据

		} else {

			//不需要关注first,因为无论第几次出现,最终最小值或者最大值都是固定值,既然没有用minBy方法,说明业务上也不关注第几次出现
			if (c == 0) { //说明是false,c只有1和0,即1表示min或者max,0表示非min或者非max,因此需要替换
				value1 = fieldAccessor.set(value1, o2);//返回的永远是value1,只是对value1的内容进行替换,替换成最小值或者最大值
			}
			return value1;
		}

	}

}
