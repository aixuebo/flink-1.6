/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.io.async;

import org.apache.flink.util.Preconditions;

import java.util.concurrent.FutureTask;

/**
 * @param <V> return type of the callable function
 * 传入call异步调用实例，参与异步调用框架处理
 */
public class AsyncStoppableTaskWithCallback<V> extends FutureTask<V> {

	protected final StoppableCallbackCallable<V> stoppableCallbackCallable;

	public AsyncStoppableTaskWithCallback(StoppableCallbackCallable<V> callable) {
		super(Preconditions.checkNotNull(callable));
		this.stoppableCallbackCallable = callable;
	}

	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		final boolean cancel = super.cancel(mayInterruptIfRunning);
		if (cancel) {
			stoppableCallbackCallable.stop();
			// this is where we report done() for the cancel case, after calling stop().
			stoppableCallbackCallable.done(true);
		}
		return cancel;
	}

	@Override
	protected void done() {
		// we suppress forwarding if we have not been canceled, because the cancel case will call to this method separately.
		if (!isCancelled()) {
			stoppableCallbackCallable.done(false);
		}
	}

	public static <V> AsyncStoppableTaskWithCallback<V> from(StoppableCallbackCallable<V> callable) {
		return new AsyncStoppableTaskWithCallback<>(callable);
	}
}
