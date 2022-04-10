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

package org.apache.flink.runtime.state.ttl;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.SupplierWithException;
import org.apache.flink.util.function.ThrowingConsumer;
import org.apache.flink.util.function.ThrowingRunnable;

/**
 * Base class for TTL logic wrappers.
 *
 * @param <T> Type of originally wrapped object
 */
abstract class AbstractTtlDecorator<T> {
	/** Wrapped original state handler. */
	final T original;

	final StateTtlConfig config;

	final TtlTimeProvider timeProvider;

	/** Whether to renew expiration timestamp on state read access. 读数据是否会更新最近访问时间戳*/
	final boolean updateTsOnRead;//true表示读操作也会更新时间戳

	/** Whether to renew expiration timestamp on state read access. 过期数据的可见性*/
	final boolean returnExpired;//true 表示过期的数据也会被返回,false表示过期数据不会被看到

	/** State value time to live in milliseconds. 过期周期,最后访问日期+ttl说明就是过期时间,超过该值就可以被删除了*/
	final long ttl;

	AbstractTtlDecorator(
		T original,
		StateTtlConfig config,
		TtlTimeProvider timeProvider) {
		Preconditions.checkNotNull(original);
		Preconditions.checkNotNull(config);
		Preconditions.checkNotNull(timeProvider);
		this.original = original;
		this.config = config;
		this.timeProvider = timeProvider;
		this.updateTsOnRead = config.getUpdateType() == StateTtlConfig.UpdateType.OnReadAndWrite;
		this.returnExpired = config.getStateVisibility() == StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp;
		this.ttl = config.getTtl().toMilliseconds();
	}

	//返回value具体的值
	//其中如果过期了,但没有被彻底删除,也会被返回value值
	<V> V getUnexpired(TtlValue<V> ttlValue) {
		return ttlValue == null || (expired(ttlValue) && !returnExpired) ? null : ttlValue.getUserValue();
	}

	//true表示已经过期
	<V> boolean expired(TtlValue<V> ttlValue) {
		return TtlUtils.expired(ttlValue, ttl, timeProvider);
	}

	//返回一个包装对象,即value+最近访问时间
	<V> TtlValue<V> wrapWithTs(V value) {
		return TtlUtils.wrapWithTs(value, timeProvider.currentTimestamp());
	}

	//返回一个包装对象,即value+最近访问时间
	<V> TtlValue<V> rewrapWithNewTs(TtlValue<V> ttlValue) {
		return wrapWithTs(ttlValue.getUserValue());
	}

	<SE extends Throwable, CE extends Throwable, CLE extends Throwable, V> V getWithTtlCheckAndUpdate(
		SupplierWithException<TtlValue<V>, SE> getter,
		ThrowingConsumer<TtlValue<V>, CE> updater,
		ThrowingRunnable<CLE> stateClear) throws SE, CE, CLE {
		TtlValue<V> ttlValue = getWrappedWithTtlCheckAndUpdate(getter, updater, stateClear);
		return ttlValue == null ? null : ttlValue.getUserValue();
	}

	<SE extends Throwable, CE extends Throwable, CLE extends Throwable, V> TtlValue<V> getWrappedWithTtlCheckAndUpdate(
		SupplierWithException<TtlValue<V>, SE> getter,
		ThrowingConsumer<TtlValue<V>, CE> updater,
		ThrowingRunnable<CLE> stateClear) throws SE, CE, CLE {
		TtlValue<V> ttlValue = getter.get();
		if (ttlValue == null) {
			return null;
		} else if (expired(ttlValue)) {
			stateClear.run();
			if (!returnExpired) {
				return null;
			}
		} else if (updateTsOnRead) {
			updater.accept(rewrapWithNewTs(ttlValue));
		}
		return ttlValue;
	}
}
