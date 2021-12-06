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

package org.apache.flink.runtime.rpc.akka;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.rpc.FencedRpcGateway;
import org.apache.flink.runtime.rpc.MainThreadExecutable;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcServer;
import org.apache.flink.runtime.rpc.RpcTimeout;
import org.apache.flink.runtime.rpc.StartStoppable;
import org.apache.flink.runtime.rpc.akka.messages.Processing;
import org.apache.flink.runtime.rpc.messages.CallAsync;
import org.apache.flink.runtime.rpc.messages.LocalRpcInvocation;
import org.apache.flink.runtime.rpc.messages.RemoteRpcInvocation;
import org.apache.flink.runtime.rpc.messages.RpcInvocation;
import org.apache.flink.runtime.rpc.messages.RunAsync;
import org.apache.flink.util.Preconditions;

import akka.actor.ActorRef;
import akka.pattern.Patterns;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Invocation handler to be used with an {@link AkkaRpcActor}. The invocation handler wraps the
 * rpc in a {@link LocalRpcInvocation} message and then sends it to the {@link AkkaRpcActor} where it is
 * executed.
 *
 * 动态代理的方式实现RPC
 *
 * 客户端持有该对象
 */
class AkkaInvocationHandler implements InvocationHandler, AkkaBasedEndpoint, RpcServer {
	private static final Logger LOG = LoggerFactory.getLogger(AkkaInvocationHandler.class);

	/**
	 * The Akka (RPC) address of {@link #rpcEndpoint} including host and port of the ActorSystem in
	 * which the actor is running.
	 * 作为客户端上的代理:远程连接的服务器的ip
	 * 作为服务器上的代理:服务器接口对应的地址,即本地地址
	 */
	private final String address;

	/**
	 * Hostname of the host, {@link #rpcEndpoint} is running on.
	 * 远程连接的服务器的host
	 */
	private final String hostname;

	//与远程服务器的连接connect对象
	private final ActorRef rpcEndpoint;//代理类，服务器在本地的代理类，或者本地接口服务对应的类

	// whether the actor ref is local and thus no message serialization is needed
	protected final boolean isLocal;//代理类是否也在同一节点上运行的,true表示远程节点 是不是就是本地节点

	// default timeout for asks
	private final Time timeout;

	private final long maximumFramesize;//最大的传输字节,超过该字节，不允许传输

	// null if gateway; otherwise non-null
	@Nullable
	private final CompletableFuture<Void> terminationFuture;//与远程对象传输的可序列化对象

	AkkaInvocationHandler(
			String address,
			String hostname,
			ActorRef rpcEndpoint,
			Time timeout,
			long maximumFramesize,
			@Nullable CompletableFuture<Void> terminationFuture) {

		this.address = Preconditions.checkNotNull(address);
		this.hostname = Preconditions.checkNotNull(hostname);
		this.rpcEndpoint = Preconditions.checkNotNull(rpcEndpoint);
		this.isLocal = this.rpcEndpoint.path().address().hasLocalScope();
		this.timeout = Preconditions.checkNotNull(timeout);
		this.maximumFramesize = maximumFramesize;
		this.terminationFuture = terminationFuture;
	}

	@Override
	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
		Class<?> declaringClass = method.getDeclaringClass();

		Object result;

		if (declaringClass.equals(AkkaBasedEndpoint.class) ||
			declaringClass.equals(Object.class) ||
			declaringClass.equals(RpcGateway.class) ||
			declaringClass.equals(StartStoppable.class) ||
			declaringClass.equals(MainThreadExecutable.class) || //当让远程服务器运行一个Runnable、Callable时,由于本身不能序列化,因此无法做到。
																//但当远程服务器和本地服务器,都在同一台服务器上,这就可以做到了.即rpcEndpoint虽然是远程服务器,但他也可以当本地服务器用
			declaringClass.equals(RpcServer.class)) {
			result = method.invoke(this, args);
		} else if (declaringClass.equals(FencedRpcGateway.class)) {
			throw new UnsupportedOperationException("AkkaInvocationHandler does not support the call FencedRpcGateway#" +
				method.getName() + ". This indicates that you retrieved a FencedRpcGateway without specifying a " +
				"fencing token. Please use RpcService#connect(RpcService, F, Time) with F being the fencing token to " +
				"retrieve a properly FencedRpcGateway.");
		} else {
			result = invokeRpc(method, args);//远程RPC调用，把需要的信息发送给远程
		}

		return result;
	}

	@Override
	public ActorRef getActorRef() {
		return rpcEndpoint;
	}

	//异步的方式执行一个任务
	@Override
	public void runAsync(Runnable runnable) {
		scheduleRunAsync(runnable, 0L);
	}

	//异步的方式执行一个任务---无返回值
	@Override
	public void scheduleRunAsync(Runnable runnable, long delayMillis) {
		checkNotNull(runnable, "runnable");
		checkArgument(delayMillis >= 0, "delay must be zero or greater");

		if (isLocal) {
			long atTimeNanos = delayMillis == 0 ? 0 : System.nanoTime() + (delayMillis * 1_000_000);
			tell(new RunAsync(runnable, atTimeNanos));//异步执行一个任务
		} else {
			throw new RuntimeException("Trying to send a Runnable to a remote actor at " +
				rpcEndpoint.path() + ". This is not supported.");//尝试发送一个Runnable去远程服务器.是不允许的，因为Runnable不支持序列化
		}
	}

	//异步的方式执行一个任务 -- 有返回值
	@Override
	public <V> CompletableFuture<V> callAsync(Callable<V> callable, Time callTimeout) {
		if (isLocal) {
			@SuppressWarnings("unchecked")
			CompletableFuture<V> resultFuture = (CompletableFuture<V>) ask(new CallAsync(callable), callTimeout);

			return resultFuture;
		} else {
			throw new RuntimeException("Trying to send a Callable to a remote actor at " +
				rpcEndpoint.path() + ". This is not supported.");//尝试发送一个Callable去远程服务器.是不允许的，因为Runnable不支持序列化
		}
	}

	@Override
	public void start() {
		rpcEndpoint.tell(Processing.START, ActorRef.noSender());
	}

	@Override
	public void stop() {
		rpcEndpoint.tell(Processing.STOP, ActorRef.noSender());
	}

	// ------------------------------------------------------------------------
	//  Private methods
	// ------------------------------------------------------------------------

	/**
	 * Invokes a RPC method by sending the RPC invocation details to the rpc endpoint.
	 *
	 * @param method to call
	 * @param args of the method call
	 * @return result of the RPC
	 * @throws Exception if the RPC invocation fails
	 */
	private Object invokeRpc(Method method, Object[] args) throws Exception {

		//带着方法名、参数类型、参数值等信息创建RPC
		String methodName = method.getName();
		Class<?>[] parameterTypes = method.getParameterTypes();
		Annotation[][] parameterAnnotations = method.getParameterAnnotations();
		Time futureTimeout = extractRpcTimeout(parameterAnnotations, args, timeout);//获取RpcTimeout注解对应的值

		final RpcInvocation rpcInvocation = createRpcInvocationMessage(methodName, parameterTypes, args);//可序列化的对象,支持客户端与服务端之间的传输

		Class<?> returnType = method.getReturnType();

		final Object result;

		if (Objects.equals(returnType, Void.TYPE)) {
			tell(rpcInvocation);//无返回值,则直接发送异步请求tell即可
			result = null;
		} else if (Objects.equals(returnType, CompletableFuture.class)) {//返回异步对象,后期程序自己获取异步对象返回值
			// execute an asynchronous call
			result = ask(rpcInvocation, futureTimeout);//设置超时时间,返回CompletableFuture对象
		} else {
			// execute a synchronous call
			CompletableFuture<?> futureResult = ask(rpcInvocation, futureTimeout);
			result = futureResult.get(futureTimeout.getSize(), futureTimeout.getUnit());//同步等待返回值
		}

		return result;
	}

	/**
	 * Create the RpcInvocation message for the given RPC.
	 *
	 * @param methodName of the RPC
	 * @param parameterTypes of the RPC
	 * @param args of the RPC
	 * @return RpcInvocation message which encapsulates the RPC details
	 * @throws IOException if we cannot serialize the RPC invocation parameters
	 * 可序列化的对象,支持客户端与服务端之间的传输
	 */
	protected RpcInvocation createRpcInvocationMessage(
			final String methodName,
			final Class<?>[] parameterTypes,
			final Object[] args) throws IOException {
		final RpcInvocation rpcInvocation;

		if (isLocal) {
			rpcInvocation = new LocalRpcInvocation(//因为是本地的执行代理,所以不需要序列化
				methodName,
				parameterTypes,
				args);
		} else {
			try {
				RemoteRpcInvocation remoteRpcInvocation = new RemoteRpcInvocation(//因为是远程的执行代理,因此需要序列化
					methodName,
					parameterTypes,
					args);

				if (remoteRpcInvocation.getSize() > maximumFramesize) {//超过了传输字节的限制
					throw new IOException("The rpc invocation size exceeds the maximum akka framesize.");
				} else {
					rpcInvocation = remoteRpcInvocation;
				}
			} catch (IOException e) {
				LOG.warn("Could not create remote rpc invocation message. Failing rpc invocation because...", e);
				throw e;
			}
		}

		return rpcInvocation;
	}

	// ------------------------------------------------------------------------
	//  Helper methods
	// ------------------------------------------------------------------------

	/**
	 * Extracts the {@link RpcTimeout} annotated rpc timeout value from the list of given method
	 * arguments. If no {@link RpcTimeout} annotated parameter could be found, then the default
	 * timeout is returned.
	 *
	 * @param parameterAnnotations Parameter annotations
	 * @param args Array of arguments
	 * @param defaultTimeout Default timeout to return if no {@link RpcTimeout} annotated parameter
	 *                       has been found  默认值
	 * @return Timeout extracted from the array of arguments or the default timeout
	 * 获取注解RpcTimeout对应的值
	 */
	private static Time extractRpcTimeout(Annotation[][] parameterAnnotations, Object[] args, Time defaultTimeout) {
		if (args != null) {
			Preconditions.checkArgument(parameterAnnotations.length == args.length);

			for (int i = 0; i < parameterAnnotations.length; i++) {
				if (isRpcTimeout(parameterAnnotations[i])) {
					if (args[i] instanceof Time) {
						return (Time) args[i];
					} else {
						throw new RuntimeException("The rpc timeout parameter must be of type " +
							Time.class.getName() + ". The type " + args[i].getClass().getName() +
							" is not supported.");
					}
				}
			}
		}

		return defaultTimeout;
	}

	/**
	 * Checks whether any of the annotations is of type {@link RpcTimeout}.
	 *
	 * @param annotations Array of annotations
	 * @return True if {@link RpcTimeout} was found; otherwise false
	 */
	private static boolean isRpcTimeout(Annotation[] annotations) {
		for (Annotation annotation : annotations) {
			if (annotation.annotationType().equals(RpcTimeout.class)) {
				return true;
			}
		}

		return false;
	}

	/**
	 * Sends the message to the RPC endpoint.
	 *
	 * @param message to send to the RPC endpoint.
	 * 向服务器异步发送信息,无返回值
	 */
	protected void tell(Object message) {
		rpcEndpoint.tell(message, ActorRef.noSender());
	}

	/**
	 * Sends the message to the RPC endpoint and returns a future containing
	 * its response.
	 *
	 * @param message to send to the RPC endpoint
	 * @param timeout time to wait until the response future is failed with a {@link TimeoutException}
	 * @return Response future
	 * 向服务器异步发送信息,但有返回值,返回值是Future模式
	 *
	 */
	protected CompletableFuture<?> ask(Object message, Time timeout) {
		return FutureUtils.toJava(
			Patterns.ask(rpcEndpoint, message, timeout.toMilliseconds()));
	}

	@Override
	public String getAddress() {
		return address;
	}

	@Override
	public String getHostname() {
		return hostname;
	}

	@Override
	public CompletableFuture<Void> getTerminationFuture() {
		return terminationFuture;
	}
}
