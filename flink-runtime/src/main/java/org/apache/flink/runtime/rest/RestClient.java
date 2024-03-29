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

package org.apache.flink.runtime.rest;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.net.SSLEngineFactory;
import org.apache.flink.runtime.rest.messages.ErrorResponseBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.util.RestClientException;
import org.apache.flink.runtime.rest.util.RestConstants;
import org.apache.flink.runtime.rest.util.RestMapperUtils;
import org.apache.flink.util.AutoCloseableAsync;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JavaType;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.netty4.io.netty.bootstrap.Bootstrap;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufInputStream;
import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInitializer;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelOption;
import org.apache.flink.shaded.netty4.io.netty.channel.SimpleChannelInboundHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.SocketChannel;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.TooLongFrameException;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.DefaultFullHttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.FullHttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpClientCodec;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaders;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpMethod;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpObjectAggregator;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpVersion;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.multipart.Attribute;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.multipart.DefaultHttpDataFactory;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.multipart.HttpPostRequestEncoder;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.multipart.MemoryAttribute;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.SslHandler;
import org.apache.flink.shaded.netty4.io.netty.handler.stream.ChunkedWriteHandler;
import org.apache.flink.shaded.netty4.io.netty.handler.timeout.IdleStateEvent;
import org.apache.flink.shaded.netty4.io.netty.handler.timeout.IdleStateHandler;
import org.apache.flink.shaded.netty4.io.netty.util.concurrent.DefaultThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE;

/**
 * This client is the counter-part to the {@link RestServerEndpoint}.
 *
 * 开启netty,发送请求,处理response内容 ClientHandler
 *
 * 主要对外提供sendRequest方法 --- 属于工具类,提供了发送请求、返回respnse的能力
 *
 * 属于客户端,需要去请求服务端(RestServerEndpoint)
 */
public class RestClient implements AutoCloseableAsync {
	private static final Logger LOG = LoggerFactory.getLogger(RestClient.class);

	private static final ObjectMapper objectMapper = RestMapperUtils.getStrictObjectMapper();//JSON对象转化类

	// used to open connections to a rest server endpoint
	private final Executor executor;//执行一个线程

	private final Bootstrap bootstrap;

	private final CompletableFuture<Void> terminationFuture;

	private final AtomicBoolean isRunning = new AtomicBoolean(true);

	public RestClient(RestClientConfiguration configuration, Executor executor) {
		Preconditions.checkNotNull(configuration);
		this.executor = Preconditions.checkNotNull(executor);
		this.terminationFuture = new CompletableFuture<>();

		final SSLEngineFactory sslEngineFactory = configuration.getSslEngineFactory();
		ChannelInitializer<SocketChannel> initializer = new ChannelInitializer<SocketChannel>() {
			@Override
			protected void initChannel(SocketChannel socketChannel) {
				try {
					// SSL should be the first handler in the pipeline
					if (sslEngineFactory != null) {//套一层ssl安全引擎
						socketChannel.pipeline().addLast("ssl", new SslHandler(sslEngineFactory.createSSLEngine()));
					}

					socketChannel.pipeline()
						.addLast(new HttpClientCodec())
						.addLast(new HttpObjectAggregator(configuration.getMaxContentLength()))
						.addLast(new ChunkedWriteHandler()) // required for multipart-requests
						.addLast(new IdleStateHandler(configuration.getIdlenessTimeout(), configuration.getIdlenessTimeout(), configuration.getIdlenessTimeout(), TimeUnit.MILLISECONDS))
						.addLast(new ClientHandler());
				} catch (Throwable t) {
					t.printStackTrace();
					ExceptionUtils.rethrow(t);
				}
			}
		};
		NioEventLoopGroup group = new NioEventLoopGroup(1, new DefaultThreadFactory("flink-rest-client-netty"));

		bootstrap = new Bootstrap();
		bootstrap
			.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, Math.toIntExact(configuration.getConnectionTimeout()))
			.group(group)
			.channel(NioSocketChannel.class)
			.handler(initializer);

		LOG.info("Rest client endpoint started.");
	}

	@Override
	public CompletableFuture<Void> closeAsync() {
		return shutdownInternally(Time.seconds(10L));
	}

	public void shutdown(Time timeout) {
		final CompletableFuture<Void> shutDownFuture = shutdownInternally(timeout);

		try {
			shutDownFuture.get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);
			LOG.info("Rest endpoint shutdown complete.");
		} catch (Exception e) {
			LOG.warn("Rest endpoint shutdown failed.", e);
		}
	}

	private CompletableFuture<Void> shutdownInternally(Time timeout) {
		if (isRunning.compareAndSet(true, false)) {
			LOG.info("Shutting down rest endpoint.");

			if (bootstrap != null) {
				if (bootstrap.group() != null) {
					bootstrap.group().shutdownGracefully(0L, timeout.toMilliseconds(), TimeUnit.MILLISECONDS)
						.addListener(finished -> {
							if (finished.isSuccess()) {
								terminationFuture.complete(null);
							} else {
								terminationFuture.completeExceptionally(finished.cause());
							}
						});
				}
			}
		}
		return terminationFuture;
	}

	public <M extends MessageHeaders<R, P, U>, U extends MessageParameters, R extends RequestBody, P extends ResponseBody> CompletableFuture<P> sendRequest(
			String targetAddress,
			int targetPort,
			M messageHeaders,
			U messageParameters,
			R request) throws IOException {
		return sendRequest(targetAddress, targetPort, messageHeaders, messageParameters, request, Collections.emptyList());
	}

	public <M extends MessageHeaders<R, P, U>, U extends MessageParameters, R extends RequestBody, P extends ResponseBody> CompletableFuture<P> sendRequest(
			String targetAddress,//目标地址
			int targetPort,//目标端口
			M messageHeaders,//请求的url、如何拼接参数
			U messageParameters,//参数具体的key=value信息集合
			R request,//请求的request对象
			Collection<FileUpload> fileUploads) throws IOException {
		Preconditions.checkNotNull(targetAddress);
		Preconditions.checkArgument(0 <= targetPort && targetPort < 65536, "The target port " + targetPort + " is not in the range (0, 65536].");
		Preconditions.checkNotNull(messageHeaders);
		Preconditions.checkNotNull(request);
		Preconditions.checkNotNull(messageParameters);
		Preconditions.checkNotNull(fileUploads);
		Preconditions.checkState(messageParameters.isResolved(), "Message parameters were not resolved.");

		String targetUrl = MessageParameters.resolveUrl(messageHeaders.getTargetRestEndpointURL(), messageParameters);//请求的url

		//发送请求request去 host:port url
		LOG.debug("Sending request of class {} to {}:{}{}", request.getClass(), targetAddress, targetPort, targetUrl);

		// serialize payload 将request请求对象转换成json信息,然后再转换成字节数组
		StringWriter sw = new StringWriter();
		objectMapper.writeValue(sw, request);
		ByteBuf payload = Unpooled.wrappedBuffer(sw.toString().getBytes(ConfigConstants.DEFAULT_CHARSET));

		Request httpRequest = createRequest(targetAddress + ':' + targetPort, targetUrl, messageHeaders.getHttpMethod().getNettyHttpMethod(), payload, fileUploads);

		final JavaType responseType;

		final Collection<Class<?>> typeParameters = messageHeaders.getResponseTypeParameters();//response输出是对象,所以需要将json转换成对象

		if (typeParameters.isEmpty()) {
			responseType = objectMapper.constructType(messageHeaders.getResponseClass());
		} else {
			responseType = objectMapper.getTypeFactory().constructParametricType(
				messageHeaders.getResponseClass(),
				typeParameters.toArray(new Class<?>[typeParameters.size()]));
		}

		return submitRequest(targetAddress, targetPort, httpRequest, responseType);
	}

	/**
	 * @param targetAddress 目标host:port
	 * @param targetUrl 目标url
	 * @param httpMethod get/post等方法
	 * @param jsonPayload 请求的request字节内容
	 * @param fileUploads 上传的文件集合  如果为"" 表示无上传文件
	 * @return 返回netty的request对象
	 * @throws IOException
	 */
	private static Request createRequest(String targetAddress, String targetUrl, HttpMethod httpMethod, ByteBuf jsonPayload, Collection<FileUpload> fileUploads) throws IOException {
		if (fileUploads.isEmpty()) {//没有上传的文件

			HttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, httpMethod, targetUrl, jsonPayload);

			httpRequest.headers()
				.set(HttpHeaders.Names.HOST, targetAddress)
				.set(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.CLOSE)
				.add(HttpHeaders.Names.CONTENT_LENGTH, jsonPayload.capacity())
				.add(HttpHeaders.Names.CONTENT_TYPE, RestConstants.REST_CONTENT_TYPE);

			return new SimpleRequest(httpRequest);
		} else {
			HttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, httpMethod, targetUrl);

			httpRequest.headers()
				.set(HttpHeaders.Names.HOST, targetAddress)
				.set(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.CLOSE);

			// takes care of splitting the request into multiple parts
			HttpPostRequestEncoder bodyRequestEncoder;
			try {
				// we could use mixed attributes here but we have to ensure that the minimum size is greater than
				// any file as the upload otherwise fails
				DefaultHttpDataFactory httpDataFactory = new DefaultHttpDataFactory(true);
				// the FileUploadHandler explicitly checks for multipart headers
				bodyRequestEncoder = new HttpPostRequestEncoder(httpDataFactory, httpRequest, true);

				Attribute requestAttribute = new MemoryAttribute(FileUploadHandler.HTTP_ATTRIBUTE_REQUEST);
				requestAttribute.setContent(jsonPayload);
				bodyRequestEncoder.addBodyHttpData(requestAttribute);

				int fileIndex = 0;
				for (FileUpload fileUpload : fileUploads) {
					Path path = fileUpload.getFile();
					if (Files.isDirectory(path)) {
						throw new IllegalArgumentException("Upload of directories is not supported. Dir=" + path);
					}
					File file = path.toFile();
					LOG.trace("Adding file {} to request.", file);
					bodyRequestEncoder.addBodyFileUpload("file_" + fileIndex, file, fileUpload.getContentType(), false);
					fileIndex++;
				}
			} catch (HttpPostRequestEncoder.ErrorDataEncoderException e) {
				throw new IOException("Could not encode request.", e);
			}

			try {
				httpRequest = bodyRequestEncoder.finalizeRequest();
			} catch (HttpPostRequestEncoder.ErrorDataEncoderException e) {
				throw new IOException("Could not finalize request.", e);
			}

			return new MultipartRequest(httpRequest, bodyRequestEncoder);
		}
	}

	//真正意义发送一个请求
	private <P extends ResponseBody> CompletableFuture<P> submitRequest(String targetAddress, int targetPort, Request httpRequest, JavaType responseType) {
		final ChannelFuture connectFuture = bootstrap.connect(targetAddress, targetPort);

		final CompletableFuture<Channel> channelFuture = new CompletableFuture<>();

		connectFuture.addListener(
			(ChannelFuture future) -> {
				if (future.isSuccess()) {
					channelFuture.complete(future.channel());
				} else {
					channelFuture.completeExceptionally(future.cause());
				}
			});

		return channelFuture
			.thenComposeAsync(
				channel -> {
					ClientHandler handler = channel.pipeline().get(ClientHandler.class);

					CompletableFuture<JsonResponse> future;
					boolean success = false;

					try {
						if (handler == null) {
							throw new IOException("Netty pipeline was not properly initialized.");
						} else {
							httpRequest.writeTo(channel);//请求输出到流中
							future = handler.getJsonFuture();
							success = true;
						}
					} catch (IOException e) {
						future = FutureUtils.completedExceptionally(new ConnectionException("Could not write request.", e));
					} finally {
						if (!success) {
							channel.close();
						}
					}

					return future;
				},
				executor)
			.thenComposeAsync(
				(JsonResponse rawResponse) -> parseResponse(rawResponse, responseType),
				executor);
	}

	//解析成Response对象 --- 把rawResponse的json形式字符串,转换成JavaType对象
	private static <P extends ResponseBody> CompletableFuture<P> parseResponse(JsonResponse rawResponse, JavaType responseType) {
		CompletableFuture<P> responseFuture = new CompletableFuture<>();
		final JsonParser jsonParser = objectMapper.treeAsTokens(rawResponse.json);
		try {
			P response = objectMapper.readValue(jsonParser, responseType);
			responseFuture.complete(response);
		} catch (IOException originalException) {
			// the received response did not matched the expected response type

			// lets see if it is an ErrorResponse instead
			try {
				ErrorResponseBody error = objectMapper.treeToValue(rawResponse.getJson(), ErrorResponseBody.class);
				responseFuture.completeExceptionally(new RestClientException(error.errors.toString(), rawResponse.getHttpResponseStatus()));
			} catch (JsonProcessingException jpe2) {
				// if this fails it is either the expected type or response type was wrong, most likely caused
				// by a client/search MessageHeaders mismatch
				LOG.error("Received response was neither of the expected type ({}) nor an error. Response={}", responseType, rawResponse, jpe2);
				responseFuture.completeExceptionally(
					new RestClientException(
						"Response was neither of the expected type(" + responseType + ") nor an error.",
						originalException,
						rawResponse.getHttpResponseStatus()));
			}
		}
		return responseFuture;
	}

	private interface Request {
		void writeTo(Channel channel) throws IOException;
	}

	private static final class SimpleRequest implements Request {
		private final HttpRequest httpRequest;

		SimpleRequest(HttpRequest httpRequest) {
			this.httpRequest = httpRequest;
		}

		@Override
		public void writeTo(Channel channel) {
			channel.writeAndFlush(httpRequest);
		}
	}

	private static final class MultipartRequest implements Request {
		private final HttpRequest httpRequest;
		private final HttpPostRequestEncoder bodyRequestEncoder;

		MultipartRequest(HttpRequest httpRequest, HttpPostRequestEncoder bodyRequestEncoder) {
			this.httpRequest = httpRequest;
			this.bodyRequestEncoder = bodyRequestEncoder;
		}

		@Override
		public void writeTo(Channel channel) {
			ChannelFuture future = channel.writeAndFlush(httpRequest);
			// this should never be false as we explicitly set the encoder to use multipart messages
			if (bodyRequestEncoder.isChunked()) {
				future = channel.writeAndFlush(bodyRequestEncoder);
			}

			// release data and remove temporary files if they were created, once the writing is complete
			future.addListener((ignored) -> bodyRequestEncoder.cleanFiles());
		}
	}

	private static class ClientHandler extends SimpleChannelInboundHandler<Object> {

		private final CompletableFuture<JsonResponse> jsonFuture = new CompletableFuture<>();

		CompletableFuture<JsonResponse> getJsonFuture() {
			return jsonFuture;
		}

		//处理response信息
		@Override
		protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
			if (msg instanceof HttpResponse && ((HttpResponse) msg).status().equals(REQUEST_ENTITY_TOO_LARGE)) {//413 Request Entity Too Large
				jsonFuture.completeExceptionally(
					new RestClientException(
						String.format(
							REQUEST_ENTITY_TOO_LARGE + ". Try to raise [%s]",
							RestOptions.CLIENT_MAX_CONTENT_LENGTH.key()),
						((HttpResponse) msg).status()));
			} else if (msg instanceof FullHttpResponse) {
				readRawResponse((FullHttpResponse) msg);
			} else {
				LOG.error("Implementation error: Received a response that wasn't a FullHttpResponse.");
				if (msg instanceof HttpResponse) {
					jsonFuture.completeExceptionally(
						new RestClientException(
							"Implementation error: Received a response that wasn't a FullHttpResponse.",
							((HttpResponse) msg).getStatus()));
				} else {
					jsonFuture.completeExceptionally(
						new RestClientException(
							"Implementation error: Received a response that wasn't a FullHttpResponse.",
							HttpResponseStatus.INTERNAL_SERVER_ERROR));
				}

			}
			ctx.close();
		}

		@Override
		public void channelInactive(ChannelHandlerContext ctx) {
			jsonFuture.completeExceptionally(new ConnectionClosedException("Channel became inactive."));
			ctx.close();
		}

		@Override
		public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
			if (evt instanceof IdleStateEvent) {
				jsonFuture.completeExceptionally(new ConnectionIdleException("Channel became idle."));
				ctx.close();
			} else {
				super.userEventTriggered(ctx, evt);
			}
		}

		@Override
		public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
			if (cause instanceof TooLongFrameException) {
				jsonFuture.completeExceptionally(new TooLongFrameException(String.format(
					cause.getMessage() + " Try to raise [%s]",
					RestOptions.CLIENT_MAX_CONTENT_LENGTH.key())));
			} else {
				jsonFuture.completeExceptionally(cause);
			}
			ctx.close();
		}

		private void readRawResponse(FullHttpResponse msg) {
			ByteBuf content = msg.content();

			JsonNode rawResponse;
			try (InputStream in = new ByteBufInputStream(content)) {
				rawResponse = objectMapper.readTree(in);
				LOG.debug("Received response {}.", rawResponse);
			} catch (JsonProcessingException je) {//不是json信息
				LOG.error("Response was not valid JSON.", je);
				// let's see if it was a plain-text message instead
				content.readerIndex(0);
				try (ByteBufInputStream in = new ByteBufInputStream(content)) {
					byte[] data = new byte[in.available()];
					in.readFully(data);
					String message = new String(data);
					LOG.error("Unexpected plain-text response: {}", message);
					jsonFuture.completeExceptionally(new RestClientException("Response was not valid JSON, but plain-text: " + message, je, msg.getStatus()));
				} catch (IOException e) {
					jsonFuture.completeExceptionally(new RestClientException("Response was not valid JSON, nor plain-text.", je, msg.getStatus()));
				}
				return;
			} catch (IOException ioe) {
				LOG.error("Response could not be read.", ioe);
				jsonFuture.completeExceptionally(new RestClientException("Response could not be read.", ioe, msg.getStatus()));
				return;
			}
			jsonFuture.complete(new JsonResponse(rawResponse, msg.getStatus()));
		}
	}

	private static final class JsonResponse {
		private final JsonNode json;//返回的json信息
		private final HttpResponseStatus httpResponseStatus;//返回的状态码

		private JsonResponse(JsonNode json, HttpResponseStatus httpResponseStatus) {
			this.json = Preconditions.checkNotNull(json);
			this.httpResponseStatus = Preconditions.checkNotNull(httpResponseStatus);
		}

		public JsonNode getJson() {
			return json;
		}

		public HttpResponseStatus getHttpResponseStatus() {
			return httpResponseStatus;
		}

		@Override
		public String toString() {
			return "JsonResponse{" +
				"json=" + json +
				", httpResponseStatus=" + httpResponseStatus +
				'}';
		}
	}
}
