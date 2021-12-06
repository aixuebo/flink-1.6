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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.concurrent.akka.ActorSystemScheduledExecutorAdapter;
import org.apache.flink.runtime.rpc.FencedMainThreadExecutable;
import org.apache.flink.runtime.rpc.FencedRpcEndpoint;
import org.apache.flink.runtime.rpc.FencedRpcGateway;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcServer;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.exceptions.RpcConnectionException;
import org.apache.flink.runtime.rpc.messages.HandshakeSuccessMessage;
import org.apache.flink.runtime.rpc.messages.RemoteHandshakeMessage;

import akka.actor.ActorIdentity;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import akka.actor.Address;
import akka.actor.Identify;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.Terminated;
import akka.dispatch.Futures;
import akka.pattern.Patterns;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import scala.Option;
import scala.concurrent.Future;
import scala.reflect.ClassTag$;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Akka based {@link RpcService} implementation. The RPC service starts an Akka actor to receive
 * RPC invocations from a {@link RpcGateway}.
 *
 * 对外开放一个服务 --- 服务端和客户端都有该服务
 */
@ThreadSafe
public class AkkaRpcService implements RpcService {

	private static final Logger LOG = LoggerFactory.getLogger(AkkaRpcService.class);

	static final int VERSION = 1;

	static final String MAXIMUM_FRAME_SIZE_PATH = "akka.remote.netty.tcp.maximum-frame-size";//最大的传输字节的key

	private final Object lock = new Object();

	private final ActorSystem actorSystem;//本节点服务
	private final Time timeout;

	//key是服务的tcp对应的serverSocket,即AkkaRpcActor,value是服务接口的实现类
	//服务器端持有该属性,表示服务器上有哪些服务被开启了
	@GuardedBy("lock")
	private final Map<ActorRef, RpcEndpoint> actors = new HashMap<>(4);

	private final long maximumFramesize;//最大的传输字节

	private final String address;//服务对外的ip+port
	private final int port;

	private final ScheduledExecutor internalScheduledExecutor;

	private final CompletableFuture<Void> terminationFuture;//stop服务后,返回的Feature对象

	private volatile boolean stopped;//是否本节点服务已经停止了

	public AkkaRpcService(final ActorSystem actorSystem, final Time timeout) {
		this.actorSystem = checkNotNull(actorSystem, "actor system");
		this.timeout = checkNotNull(timeout, "timeout");

		if (actorSystem.settings().config().hasPath(MAXIMUM_FRAME_SIZE_PATH)) {//最大的传输字节的key
			maximumFramesize = actorSystem.settings().config().getBytes(MAXIMUM_FRAME_SIZE_PATH);
		} else {
			// only local communication
			maximumFramesize = Long.MAX_VALUE;
		}

		Address actorSystemAddress = AkkaUtils.getAddress(actorSystem);

		if (actorSystemAddress.host().isDefined()) {
			address = actorSystemAddress.host().get();
		} else {
			address = "";
		}

		if (actorSystemAddress.port().isDefined()) {
			port = (Integer) actorSystemAddress.port().get();
		} else {
			port = -1;
		}

		internalScheduledExecutor = new ActorSystemScheduledExecutorAdapter(actorSystem);

		terminationFuture = new CompletableFuture<>();

		stopped = false;
	}

	public ActorSystem getActorSystem() {
		return actorSystem;
	}

	protected int getVersion() {
		return VERSION;
	}

	@Override
	public String getAddress() {
		return address;
	}

	@Override
	public int getPort() {
		return port;
	}

	// this method does not mutate state and is thus thread-safe
	//返回一个服务器远程代理类 --- 该返回值只是一个服务器,该服务器上部署很多接口,当访问接口的时候,需要startServer方法
	@Override
	public <C extends RpcGateway> CompletableFuture<C> connect(
			final String address,
			final Class<C> clazz) {

		return connectInternal(
			address,
			clazz,
			(ActorRef actorRef) -> {//传入actorRef,输出AkkaInvocationHandler
				Tuple2<String, String> addressHostname = extractAddressHostname(actorRef);

				return new AkkaInvocationHandler(
					addressHostname.f0,
					addressHostname.f1,
					actorRef,
					timeout,
					maximumFramesize,
					null);
			});
	}

	// this method does not mutate state and is thus thread-safe

	/**
	 * 请求远程服务器,返回网关对应的实例化对象
	 * @param address Address of the remote rpc server 服务器地址
	 * @param fencingToken Fencing token to be used when communicating with the server 与服务器交互的对象
	 * @param clazz Class of the rpc gateway to return 要返回的对象
	 * @return
	 *
	 * F extends Serializable,与服务器交互的对象是支持序列化的
	 * C extends FencedRpcGateway<F> 返回的对象是网关对象的子类,并且该网关是接受F序列化对象的
	 */
	@Override
	public <F extends Serializable, C extends FencedRpcGateway<F>> CompletableFuture<C> connect(String address, F fencingToken, Class<C> clazz) {
		return connectInternal(
			address,
			clazz,
			(ActorRef actorRef) -> {//传入actorRef,输出FencedAkkaInvocationHandler
				Tuple2<String, String> addressHostname = extractAddressHostname(actorRef);//获取远程服务器的ip+host信息

				return new FencedAkkaInvocationHandler<>(
					addressHostname.f0,
					addressHostname.f1,
					actorRef,
					timeout,
					maximumFramesize,
					null,
					() -> fencingToken);
			});
	}

	/**
	 * 服务器上具体开启一个接口服务，该服务可以对外提供远程RPC调用---服务度上调用该方法
	 * @param rpcEndpoint Rpc protocol to dispatch the rpcs to 接口实现类--用于当服务器
	 * @param <C>
	 * @return 返回对外开放的一个服务器
	 */
	@Override
	public <C extends RpcEndpoint & RpcGateway> RpcServer startServer(C rpcEndpoint) {
		checkNotNull(rpcEndpoint, "rpc endpoint");

		CompletableFuture<Void> terminationFuture = new CompletableFuture<>();
		final Props akkaRpcActorProps;//创建一个代理该服务的代理类

		if (rpcEndpoint instanceof FencedRpcEndpoint) {
			akkaRpcActorProps = Props.create(FencedAkkaRpcActor.class, rpcEndpoint, terminationFuture, getVersion());//创建class,以及class需要的对应的参数
		} else {
			akkaRpcActorProps = Props.create(AkkaRpcActor.class, rpcEndpoint, terminationFuture, getVersion());//构造AkkaRpcActor对象,后面是需要的参数
		}

		ActorRef actorRef;//AkkaRpcActor实例的一个引用,该引用相当于tcp的serversocket

		synchronized (lock) {
			checkState(!stopped, "RpcService is stopped");
			actorRef = actorSystem.actorOf(akkaRpcActorProps, rpcEndpoint.getEndpointId());//创建一个AkkaRpcActor class实例引用
			actors.put(actorRef, rpcEndpoint);//key是服务tcp,value是服务接口对应实现类
		}

		LOG.info("Starting RPC endpoint for {} at {} .", rpcEndpoint.getClass().getName(), actorRef.path());

		//该接口服务对外开放的地址
		final String akkaAddress = AkkaUtils.getAkkaURL(actorSystem, actorRef);
		final String hostname;
		Option<String> host = actorRef.path().address().host();
		if (host.isEmpty()) {
			hostname = "localhost";
		} else {
			hostname = host.get();
		}

		Set<Class<?>> implementedRpcGateways = new HashSet<>(RpcUtils.extractImplementedRpcGateways(rpcEndpoint.getClass()));

		implementedRpcGateways.add(RpcServer.class);
		implementedRpcGateways.add(AkkaBasedEndpoint.class);


		//代理反射类
		final InvocationHandler akkaInvocationHandler;

		if (rpcEndpoint instanceof FencedRpcEndpoint) {
			// a FencedRpcEndpoint needs a FencedAkkaInvocationHandler
			akkaInvocationHandler = new FencedAkkaInvocationHandler<>(
				akkaAddress,
				hostname,
				actorRef,
				timeout,
				maximumFramesize,
				terminationFuture,
				((FencedRpcEndpoint<?>) rpcEndpoint)::getFencingToken);

			implementedRpcGateways.add(FencedMainThreadExecutable.class);
		} else {
			akkaInvocationHandler = new AkkaInvocationHandler(
				akkaAddress,
				hostname,
				actorRef,
				timeout,
				maximumFramesize,
				terminationFuture);
		}

		// Rather than using the System ClassLoader directly, we derive the ClassLoader
		// from this class . That works better in cases where Flink runs embedded and all Flink
		// code is loaded dynamically (for example from an OSGI bundle) through a custom ClassLoader
		ClassLoader classLoader = getClass().getClassLoader();

		//返回服务的代理类
		@SuppressWarnings("unchecked")
		RpcServer server = (RpcServer) Proxy.newProxyInstance(
			classLoader,
			implementedRpcGateways.toArray(new Class<?>[implementedRpcGateways.size()]),
			akkaInvocationHandler);

		return server;
	}

	@Override
	public <F extends Serializable> RpcServer fenceRpcServer(RpcServer rpcServer, F fencingToken) {
		if (rpcServer instanceof AkkaBasedEndpoint) {

			InvocationHandler fencedInvocationHandler = new FencedAkkaInvocationHandler<>(
				rpcServer.getAddress(),
				rpcServer.getHostname(),
				((AkkaBasedEndpoint) rpcServer).getActorRef(),
				timeout,
				maximumFramesize,
				null,
				() -> fencingToken);

			// Rather than using the System ClassLoader directly, we derive the ClassLoader
			// from this class . That works better in cases where Flink runs embedded and all Flink
			// code is loaded dynamically (for example from an OSGI bundle) through a custom ClassLoader
			ClassLoader classLoader = getClass().getClassLoader();

			return (RpcServer) Proxy.newProxyInstance(
				classLoader,
				new Class<?>[]{RpcServer.class, AkkaBasedEndpoint.class},
				fencedInvocationHandler);
		} else {
			throw new RuntimeException("The given RpcServer must implement the AkkaGateway in order to fence it.");
		}
	}

	@Override
	public void stopServer(RpcServer selfGateway) {
		if (selfGateway instanceof AkkaBasedEndpoint) {
			final AkkaBasedEndpoint akkaClient = (AkkaBasedEndpoint) selfGateway;
			final RpcEndpoint rpcEndpoint;

			synchronized (lock) {
				if (stopped) {
					return;
				} else {
					rpcEndpoint = actors.remove(akkaClient.getActorRef());
				}
			}

			if (rpcEndpoint != null) {
				akkaClient.getActorRef().tell(PoisonPill.getInstance(), ActorRef.noSender());
			} else {
				LOG.debug("RPC endpoint {} already stopped or from different RPC service", selfGateway.getAddress());
			}
		}
	}

	@Override
	public CompletableFuture<Void> stopService() {
		synchronized (lock) {
			if (stopped) {
				return terminationFuture;
			}

			stopped = true;
		}

		LOG.info("Stopping Akka RPC service.");

		final CompletableFuture<Terminated> actorSystemTerminationFuture = FutureUtils.toJava(actorSystem.terminate());

		actorSystemTerminationFuture.whenComplete(
			(Terminated ignored, Throwable throwable) -> {
				synchronized (lock) {
					actors.clear();
				}

				if (throwable != null) {
					terminationFuture.completeExceptionally(throwable);
				} else {
					terminationFuture.complete(null);
				}

				LOG.info("Stopped Akka RPC service.");
			});

		return terminationFuture;
	}

	@Override
	public CompletableFuture<Void> getTerminationFuture() {
		return terminationFuture;
	}

	//返回一个线程执行器
	@Override
	public Executor getExecutor() {
		return actorSystem.dispatcher();
	}

	//返回本地的定时调度器
	@Override
	public ScheduledExecutor getScheduledExecutor() {
		return internalScheduledExecutor;
	}

	//定时调度执行runnable
	@Override
	public ScheduledFuture<?> scheduleRunnable(Runnable runnable, long delay, TimeUnit unit) {
		checkNotNull(runnable, "runnable");
		checkNotNull(unit, "unit");
		checkArgument(delay >= 0L, "delay must be zero or larger");

		return internalScheduledExecutor.schedule(runnable, delay, unit);
	}

	//具体执行一个无返回值的Runnable调度任务
	@Override
	public void execute(Runnable runnable) {
		actorSystem.dispatcher().execute(runnable);
	}

	//具体执行一个有返回值的Callable调度任务
	@Override
	public <T> CompletableFuture<T> execute(Callable<T> callable) {
		Future<T> scalaFuture = Futures.<T>future(callable, actorSystem.dispatcher());//actorSystem.dispatcher() 返回Executor线程执行器

		return FutureUtils.toJava(scalaFuture);
	}

	// ---------------------------------------------------------------------------------------
	// Private helper methods
	// ---------------------------------------------------------------------------------------

	/**
	 * 参数是已经连接到远程服务器endpoint的对象
	 * 因此返回值是远程服务器的ip地址、host
	 */
	private Tuple2<String, String> extractAddressHostname(ActorRef actorRef) {
		final String actorAddress = AkkaUtils.getAkkaURL(actorSystem, actorRef);
		final String hostname;
		Option<String> host = actorRef.path().address().host();
		if (host.isEmpty()) {
			hostname = "localhost";
		} else {
			hostname = host.get();
		}

		return Tuple2.of(actorAddress, hostname);
	}

	//连接服务器,请求服务器的某一个接口(class可以确定接口),返回信息可以动态代理的方式实现PRC功能
	private <C extends RpcGateway> CompletableFuture<C> connectInternal(
			final String address,
			final Class<C> clazz,
			Function<ActorRef, InvocationHandler> invocationHandlerFactory) {//函数,传入ActorRef,输出InvocationHandler
		checkState(!stopped, "RpcService is stopped");

		LOG.debug("Try to connect to remote RPC endpoint with address {}. Returning a {} gateway.",
			address, clazz.getName());

		//远程服务器的引用
		final ActorSelection actorSel = actorSystem.actorSelection(address);//连接远程服务器,返回连接,可以从该连接中获取信息,信息在box里存储着

		final Future<ActorIdentity> identify = Patterns
			.ask(actorSel, new Identify(42), timeout.toMilliseconds())
			.<ActorIdentity>mapTo(ClassTag$.MODULE$.<ActorIdentity>apply(ActorIdentity.class));

		final CompletableFuture<ActorIdentity> identifyFuture = FutureUtils.toJava(identify);

		final CompletableFuture<ActorRef> actorRefFuture = identifyFuture.thenApply(
			(ActorIdentity actorIdentity) -> {
				if (actorIdentity.getRef() == null) {
					throw new CompletionException(new RpcConnectionException("Could not connect to rpc endpoint under address " + address + '.'));
				} else {
					return actorIdentity.getRef();
				}
			});

		//ask请求,并且有返回值 --- 返回是否有该class接口的网关
		final CompletableFuture<HandshakeSuccessMessage> handshakeFuture = actorRefFuture.thenCompose(
			(ActorRef actorRef) -> FutureUtils.toJava(
				Patterns
					.ask(actorRef, new RemoteHandshakeMessage(clazz, getVersion()), timeout.toMilliseconds())//请求远程服务哪个网关对象
					.<HandshakeSuccessMessage>mapTo(ClassTag$.MODULE$.<HandshakeSuccessMessage>apply(HandshakeSuccessMessage.class)))); //确保请求结果成功

		return actorRefFuture.thenCombineAsync(
			handshakeFuture,
			(ActorRef actorRef, HandshakeSuccessMessage ignored) -> {
				InvocationHandler invocationHandler = invocationHandlerFactory.apply(actorRef);

				// Rather than using the System ClassLoader directly, we derive the ClassLoader
				// from this class . That works better in cases where Flink runs embedded and all Flink
				// code is loaded dynamically (for example from an OSGI bundle) through a custom ClassLoader
				ClassLoader classLoader = getClass().getClassLoader();

				//返回class的动态代理
				@SuppressWarnings("unchecked")
				C proxy = (C) Proxy.newProxyInstance(
					classLoader,
					new Class<?>[]{clazz},
					invocationHandler);

				return proxy;
			},
			actorSystem.dispatcher());
	}
}
