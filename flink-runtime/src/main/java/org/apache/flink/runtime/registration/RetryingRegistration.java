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

package org.apache.flink.runtime.registration;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.rpc.FencedRpcGateway;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.util.ExceptionUtils;

import org.slf4j.Logger;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;


/**
 * This utility class implements the basis of registering one component at another component,
 * for example registering the TaskExecutor at the ResourceManager.
 * This {@code RetryingRegistration} implements both the initial address resolution
 * and the retries-with-backoff strategy.
 *
 * <p>The registration gives access to a future that is completed upon successful registration.
 * The registration can be canceled, for example when the target where it tries to register
 * at looses leader status.
 *
 * @param <F> The type of the fencing token 与目标服务器通讯的对象实体，比如连接ResourceManager，则该实体就是ResourceManagerId对象
 * @param <G> The type of the gateway to connect to.目标服务器的网关，比如连接ResourceManager,那就是ResourceManager的网关
 * @param <S> The type of the successful registration responses.注册成功后的返回值对象
 *
 * 尝试去真实的做连接交互请求
 */
public abstract class RetryingRegistration<F extends Serializable, G extends RpcGateway, S extends RegistrationResponse.Success> {

	// ------------------------------------------------------------------------
	//  default configuration values
	// ------------------------------------------------------------------------

	/** Default value for the initial registration timeout (milliseconds). */
	private static final long INITIAL_REGISTRATION_TIMEOUT_MILLIS = 100;

	/** Default value for the maximum registration timeout, after exponential back-off (milliseconds). */
	private static final long MAX_REGISTRATION_TIMEOUT_MILLIS = 30000;

	/** The pause (milliseconds) made after an registration attempt caused an exception (other than timeout). */
	private static final long ERROR_REGISTRATION_DELAY_MILLIS = 10000;

	/** The pause (milliseconds) made after the registration attempt was refused. */
	private static final long REFUSED_REGISTRATION_DELAY_MILLIS = 30000;

	// ------------------------------------------------------------------------
	// Fields
	// ------------------------------------------------------------------------

	private final Logger log;

	private final RpcService rpcService;

	private final String targetName;

	private final Class<G> targetType;

	private final String targetAddress;

	private final F fencingToken;

	private final CompletableFuture<Tuple2<G, S>> completionFuture;//注册成功,返回<服务器网关代理类,返回值>

	private final long initialRegistrationTimeout;//注册过程中超时时间

	private final long maxRegistrationTimeout;//

	private final long delayOnError;//错误原因造成的失败,晚一些再来一次注册

	private final long delayOnRefusedRegistration;//拒绝原因造成的失败,晚一些再来一次注册

	private volatile boolean canceled;

	// ------------------------------------------------------------------------

	public RetryingRegistration(
			Logger log,
			RpcService rpcService,
			String targetName,//为一次连接起一个名字,比如ResourceManager
			Class<G> targetType,//连接的网关
			String targetAddress,//服务器地址
			F fencingToken) {//传输的交互实体
		this(log, rpcService, targetName, targetType, targetAddress, fencingToken,
				INITIAL_REGISTRATION_TIMEOUT_MILLIS, MAX_REGISTRATION_TIMEOUT_MILLIS,
				ERROR_REGISTRATION_DELAY_MILLIS, REFUSED_REGISTRATION_DELAY_MILLIS);
	}

	public RetryingRegistration(
			Logger log,
			RpcService rpcService,
			String targetName,
			Class<G> targetType,
			String targetAddress,
			F fencingToken,
			long initialRegistrationTimeout,
			long maxRegistrationTimeout,
			long delayOnError,
			long delayOnRefusedRegistration) {

		checkArgument(initialRegistrationTimeout > 0, "initial registration timeout must be greater than zero");
		checkArgument(maxRegistrationTimeout > 0, "maximum registration timeout must be greater than zero");
		checkArgument(delayOnError >= 0, "delay on error must be non-negative");
		checkArgument(delayOnRefusedRegistration >= 0, "delay on refused registration must be non-negative");

		this.log = checkNotNull(log);
		this.rpcService = checkNotNull(rpcService);
		this.targetName = checkNotNull(targetName);
		this.targetType = checkNotNull(targetType);
		this.targetAddress = checkNotNull(targetAddress);
		this.fencingToken = checkNotNull(fencingToken);
		this.initialRegistrationTimeout = initialRegistrationTimeout;
		this.maxRegistrationTimeout = maxRegistrationTimeout;
		this.delayOnError = delayOnError;
		this.delayOnRefusedRegistration = delayOnRefusedRegistration;

		this.completionFuture = new CompletableFuture<>();
	}

	// ------------------------------------------------------------------------
	//  completion and cancellation
	// ------------------------------------------------------------------------

	public CompletableFuture<Tuple2<G, S>> getFuture() {
		return completionFuture;
	}

	/**
	 * Cancels the registration procedure.
	 * 取消注册任务
	 */
	public void cancel() {
		canceled = true;
		completionFuture.cancel(false);
	}

	/**
	 * Checks if the registration was canceled.
	 * @return True if the registration was canceled, false otherwise.
	 */
	public boolean isCanceled() {
		return canceled;
	}

	// ------------------------------------------------------------------------
	//  registration
	// ------------------------------------------------------------------------
    //真实去发生动作
	protected abstract CompletableFuture<RegistrationResponse> invokeRegistration(
		G gateway, F fencingToken, long timeoutMillis) throws Exception;

	/**
	 * This method resolves the target address to a callable gateway and starts the
	 * registration after that.
	 * 开始注册任务 --- 连接远程服务器网关
	 */
	@SuppressWarnings("unchecked")
	public void startRegistration() {
		if (canceled) {
			// we already got canceled
			return;
		}

		try {
			// trigger resolution of the resource manager address to a callable gateway
			final CompletableFuture<G> resourceManagerFuture;

			if (FencedRpcGateway.class.isAssignableFrom(targetType)) {
				resourceManagerFuture = (CompletableFuture<G>) rpcService.connect(
					targetAddress,
					fencingToken,
					targetType.asSubclass(FencedRpcGateway.class));
			} else {
				resourceManagerFuture = rpcService.connect(targetAddress, targetType);//产生socket连接,连接服务器地址上的网关class对象
			}

			// upon success, start the registration attempts 该对象表示尝试开始去注册
			CompletableFuture<Void> resourceManagerAcceptFuture = resourceManagerFuture.thenAcceptAsync(//此时说明已经连接完成
				(G result) -> {//返回值是远程服务器网关的代理类
					log.info("Resolved {} address, beginning registration", targetName);//打印日志,准备开始注册行为
					register(result, 1, initialRegistrationTimeout);//开始注册,设置尝试次数 && 超时时间
				},
				rpcService.getExecutor());

			// upon failure, retry, unless this is cancelled
			resourceManagerAcceptFuture.whenCompleteAsync(//说明已经完成了注册的请求,要么成功,要么失败
				(Void v, Throwable failure) -> {
					if (failure != null && !canceled) {//说明失败
						final Throwable strippedFailure = ExceptionUtils.stripCompletionException(failure);
						if (log.isDebugEnabled()) {
							log.debug(
								"Could not resolve {} address {}, retrying in {} ms.",
								targetName,
								targetAddress,
								delayOnError,
								strippedFailure);
						} else {
							log.info(
								"Could not resolve {} address {}, retrying in {} ms: {}.",
								targetName,
								targetAddress,
								delayOnError,
								strippedFailure.getMessage());
						}

						startRegistrationLater(delayOnError);//晚一些再来一次注册
					}
				},
				rpcService.getExecutor());
		}
		catch (Throwable t) {
			completionFuture.completeExceptionally(t);
			cancel();
		}
	}

	/**
	 * This method performs a registration attempt and triggers either a success notification or a retry,
	 * depending on the result.
	 * 准备向远程网关G发送注册请求,参数是 尝试次数 以及 超时时间
	 */
	@SuppressWarnings("unchecked")
	private void register(final G gateway, final int attempt, final long timeoutMillis) {
		// eager check for canceling to avoid some unnecessary work
		if (canceled) {
			return;
		}

		try {
			log.info("Registration at {} attempt {} (timeout={}ms)", targetName, attempt, timeoutMillis);
			CompletableFuture<RegistrationResponse> registrationFuture = invokeRegistration(gateway, fencingToken, timeoutMillis);//执行注册

			// if the registration was successful, let the TaskExecutor know
			//注册失败时,尝试重新再次注册
			CompletableFuture<Void> registrationAcceptFuture = registrationFuture.thenAcceptAsync(//说明已经开始注册成功了
				(RegistrationResponse result) -> {//获取注册的返回值
					if (!isCanceled()) {
						if (result instanceof RegistrationResponse.Success) {//注册成功
							// registration successful!
							S success = (S) result;//强转返回值
							completionFuture.complete(Tuple2.of(gateway, success));
						}
						else {//注册失败
							// registration refused or unknown
							if (result instanceof RegistrationResponse.Decline) {
								RegistrationResponse.Decline decline = (RegistrationResponse.Decline) result;
								log.info("Registration at {} was declined: {}", targetName, decline.getReason());
							} else {
								log.error("Received unknown response to registration attempt: {}", result);
							}

							log.info("Pausing and re-attempting registration in {} ms", delayOnRefusedRegistration);
							registerLater(gateway, 1, initialRegistrationTimeout, delayOnRefusedRegistration);//再次重新注册
						}
					}
				},
				rpcService.getExecutor());

			// upon failure, retry
			registrationAcceptFuture.whenCompleteAsync(
				(Void v, Throwable failure) -> {
					if (failure != null && !isCanceled()) {//再次注册依然失败
						if (ExceptionUtils.stripCompletionException(failure) instanceof TimeoutException) {//超时,则在注册
							// we simply have not received a response in time. maybe the timeout was
							// very low (initial fast registration attempts), maybe the target endpoint is
							// currently down.
							if (log.isDebugEnabled()) {
								log.debug("Registration at {} ({}) attempt {} timed out after {} ms",
									targetName, targetAddress, attempt, timeoutMillis);
							}

							long newTimeoutMillis = Math.min(2 * timeoutMillis, maxRegistrationTimeout);
							register(gateway, attempt + 1, newTimeoutMillis);
						}
						else {
							// a serious failure occurred. we still should not give up, but keep trying
							log.error("Registration at {} failed due to an error", targetName, failure);
							log.info("Pausing and re-attempting registration in {} ms", delayOnError);

							registerLater(gateway, 1, initialRegistrationTimeout, delayOnError);
						}
					} //省略else,说明注册已经成功
				},
				rpcService.getExecutor());
		}
		catch (Throwable t) {
			completionFuture.completeExceptionally(t);
			cancel();
		}
	}

	private void registerLater(final G gateway, final int attempt, final long timeoutMillis, long delay) {
		rpcService.scheduleRunnable(new Runnable() {
			@Override
			public void run() {
				register(gateway, attempt, timeoutMillis);
			}
		}, delay, TimeUnit.MILLISECONDS);
	}

	//晚一些再来一次注册
	private void startRegistrationLater(final long delay) {
		rpcService.scheduleRunnable(
			this::startRegistration,
			delay,
			TimeUnit.MILLISECONDS);
	}
}
