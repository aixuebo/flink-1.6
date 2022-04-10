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

package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.NetworkClientHandler;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferListener;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.netty.exception.LocalTransportException;
import org.apache.flink.runtime.io.network.netty.exception.RemoteTransportException;
import org.apache.flink.runtime.io.network.netty.exception.TransportException;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;

import org.apache.flink.shaded.guava18.com.google.common.collect.Maps;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Channel handler to read the messages of buffer response or error response from the
 * producer.
 *
 * <p>It is used in the old network mode.
 * 属于read时的拦截器，读取服务端返回的response信息NettyMessage
 *
 * 我们以客户端获取到响应之后回调该处理器的channelRead方法为入口来进行分析：
 */
class PartitionRequestClientHandler extends ChannelInboundHandlerAdapter implements NetworkClientHandler {

	private static final Logger LOG = LoggerFactory.getLogger(PartitionRequestClientHandler.class);

	//存储每一个服务器返回的文件流  key是本地持有channelID  value是channelID对应的RemoteInputChannel对象
	private final ConcurrentMap<InputChannelID, RemoteInputChannel> inputChannels = new ConcurrentHashMap<InputChannelID, RemoteInputChannel>();

	private final AtomicReference<Throwable> channelError = new AtomicReference<Throwable>();

	private final BufferListenerTask bufferListener = new BufferListenerTask();

	private final Queue<Object> stagedMessages = new ArrayDeque<Object>();//缓存尚未解码成NettyMessage的消息

	private final StagedMessagesHandlerTask stagedMessagesHandler = new StagedMessagesHandlerTask();

	/**
	 * Set of cancelled partition requests. A request is cancelled iff an input channel is cleared
	 * while data is still coming in for this channel.
	 */
	private final ConcurrentMap<InputChannelID, InputChannelID> cancelled = Maps.newConcurrentMap();

	private volatile ChannelHandlerContext ctx;

	// ------------------------------------------------------------------------
	// Input channel/receiver registration
	// ------------------------------------------------------------------------

	//未来该RemoteInputChannel会被response返回
	@Override
	public void addInputChannel(RemoteInputChannel listener) throws IOException {
		checkError();

		inputChannels.putIfAbsent(listener.getInputChannelId(), listener);
	}

	@Override
	public void removeInputChannel(RemoteInputChannel listener) {
		inputChannels.remove(listener.getInputChannelId());
	}

	//通知服务端 渠道id已经被取消
	@Override
	public void cancelRequestFor(InputChannelID inputChannelId) {
		if (inputChannelId == null || ctx == null) {
			return;
		}

		if (cancelled.putIfAbsent(inputChannelId, inputChannelId) == null) {
			ctx.writeAndFlush(new NettyMessage.CancelPartitionRequest(inputChannelId));
		}
	}

	@Override
	public void notifyCreditAvailable(final RemoteInputChannel inputChannel) {
	}

	// ------------------------------------------------------------------------
	// Network events
	// ------------------------------------------------------------------------

	@Override
	public void channelActive(final ChannelHandlerContext ctx) throws Exception {
		if (this.ctx == null) {
			this.ctx = ctx;
		}

		super.channelActive(ctx);
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		// Unexpected close. In normal operation, the client closes the connection after all input
		// channels have been removed. This indicates a problem with the remote task manager.
		if (!inputChannels.isEmpty()) {
			final SocketAddress remoteAddr = ctx.channel().remoteAddress();

			notifyAllChannelsOfErrorAndClose(new RemoteTransportException(
					"Connection unexpectedly closed by remote task manager '" + remoteAddr + "'. "
							+ "This might indicate that the remote task manager was lost.",
					remoteAddr));
		}

		super.channelInactive(ctx);
	}

	/**
	 * Called on exceptions in the client handler pipeline.
	 *
	 * <p> Remote exceptions are received as regular payload.
	 */
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {

		if (cause instanceof TransportException) {
			notifyAllChannelsOfErrorAndClose(cause);
		}
		else {
			final SocketAddress remoteAddr = ctx.channel().remoteAddress();

			final TransportException tex;

			// Improve on the connection reset by peer error message
			if (cause instanceof IOException
					&& cause.getMessage().equals("Connection reset by peer")) {

				tex = new RemoteTransportException(
						"Lost connection to task manager '" + remoteAddr + "'. This indicates "
								+ "that the remote task manager was lost.", remoteAddr, cause);
			}
			else {
				SocketAddress localAddr = ctx.channel().localAddress();
				tex = new LocalTransportException(
					String.format("%s (connection to '%s')", cause.getMessage(), remoteAddr),
					localAddr,
					cause);
			}

			notifyAllChannelsOfErrorAndClose(tex);
		}
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		try {
			//当没有待解析的原始消息时，直接解码消息，否则将消息加入到stagedMessages队列中，等待排队处理
			if (!bufferListener.hasStagedBufferOrEvent() && stagedMessages.isEmpty()) {
				decodeMsg(msg, false);//直接解码
			}
			else {
				stagedMessages.add(msg);//尚未解码成NettyMessage的消息
			}
		}
		catch (Throwable t) {
			notifyAllChannelsOfErrorAndClose(t);
		}
	}

	private void notifyAllChannelsOfErrorAndClose(Throwable cause) {
		if (channelError.compareAndSet(null, cause)) {
			try {
				for (RemoteInputChannel inputChannel : inputChannels.values()) {
					inputChannel.onError(cause);
				}
			}
			catch (Throwable t) {
				// We can only swallow the Exception at this point. :(
				LOG.warn("An Exception was thrown during error notification of a "
						+ "remote input channel.", t);
			}
			finally {
				inputChannels.clear();

				if (ctx != null) {
					ctx.close();
				}
			}
		}
	}

	// ------------------------------------------------------------------------

	/**
	 * Checks for an error and rethrows it if one was reported.
	 */
	private void checkError() throws IOException {
		final Throwable t = channelError.get();

		if (t != null) {
			if (t instanceof IOException) {
				throw (IOException) t;
			}
			else {
				throw new IOException("There has been an error in the channel.", t);
			}
		}
	}

	@Override
	public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
		super.channelReadComplete(ctx);
	}

	//因为已经经过解码器,因此到ClientHandler时,一定是NettyMessage对象
	//该方法就是从NettyMessage对象中进一步解析数据
	private boolean decodeMsg(Object msg, boolean isStagedBuffer) throws Throwable {
		final Class<?> msgClazz = msg.getClass();

		// ---- Buffer --------------------------------------------------------
		if (msgClazz == NettyMessage.BufferResponse.class) {//说明是服务器返回的内容
			NettyMessage.BufferResponse bufferOrEvent = (NettyMessage.BufferResponse) msg;

			RemoteInputChannel inputChannel = inputChannels.get(bufferOrEvent.receiverId);//找到接收返回值的流
			if (inputChannel == null) {//说明本地已经不需要该流了
				bufferOrEvent.releaseBuffer();

				cancelRequestFor(bufferOrEvent.receiverId);//通知服务器也删除该流

				return true;
			}

			return decodeBufferOrEvent(inputChannel, bufferOrEvent, isStagedBuffer);
		}
		// ---- Error ---------------------------------------------------------
		else if (msgClazz == NettyMessage.ErrorResponse.class) {//说明执行过程中出现问题了
			NettyMessage.ErrorResponse error = (NettyMessage.ErrorResponse) msg;

			SocketAddress remoteAddr = ctx.channel().remoteAddress();

			if (error.isFatalError()) {
				notifyAllChannelsOfErrorAndClose(new RemoteTransportException(
						"Fatal error at remote task manager '" + remoteAddr + "'.",
						remoteAddr, error.cause));
			}
			else {
				RemoteInputChannel inputChannel = inputChannels.get(error.receiverId);

				if (inputChannel != null) {
					if (error.cause.getClass() == PartitionNotFoundException.class) {
						inputChannel.onFailedPartitionRequest();
					}
					else {
						inputChannel.onError(new RemoteTransportException(
								"Error at remote task manager '" + remoteAddr + "'.",
										remoteAddr, error.cause));
					}
				}
			}
		}
		else {
			throw new IllegalStateException("Received unknown message from producer: " + msg.getClass());
		}

		return true;
	}

	/**
	 *
	 * @param inputChannel 本地要持有response的信息流
	 * @param bufferOrEvent 真实的response返回的数据对象
	 * @param isStagedBuffer
	 * @return 将数据内容bufferOrEvent 填充到RemoteInputChannel中
	 * @throws Throwable
	 */
	private boolean decodeBufferOrEvent(RemoteInputChannel inputChannel, NettyMessage.BufferResponse bufferOrEvent, boolean isStagedBuffer) throws Throwable {
		boolean releaseNettyBuffer = true;

		try {
			ByteBuf nettyBuffer = bufferOrEvent.getNettyBuffer();
			final int receivedSize = nettyBuffer.readableBytes();
			if (bufferOrEvent.isBuffer()) {
				// ---- Buffer ------------------------------------------------

				// Early return for empty buffers. Otherwise Netty's readBytes() throws an
				// IndexOutOfBoundsException.
				if (receivedSize == 0) {
					inputChannel.onEmptyBuffer(bufferOrEvent.sequenceNumber, -1);
					return true;
				}

				BufferProvider bufferProvider = inputChannel.getBufferProvider();

				if (bufferProvider == null) {
					// receiver has been cancelled/failed
					cancelRequestFor(bufferOrEvent.receiverId);
					return isStagedBuffer;
				}

				while (true) {
					Buffer buffer = bufferProvider.requestBuffer();//不断的获取buffer容器,用于接收response内容

					if (buffer != null) {
						nettyBuffer.readBytes(buffer.asByteBuf(), receivedSize);//将数据填充到buffer里

						inputChannel.onBuffer(buffer, bufferOrEvent.sequenceNumber, -1);

						return true;
					}
					else if (bufferListener.waitForBuffer(bufferProvider, bufferOrEvent)) {
						releaseNettyBuffer = false;

						return false;
					}
					else if (bufferProvider.isDestroyed()) {
						return isStagedBuffer;
					}
				}
			}
			else {
				// ---- Event -------------------------------------------------
				// TODO We can just keep the serialized data in the Netty buffer and release it later at the reader
				byte[] byteArray = new byte[receivedSize];
				nettyBuffer.readBytes(byteArray);

				MemorySegment memSeg = MemorySegmentFactory.wrap(byteArray);
				Buffer buffer = new NetworkBuffer(memSeg, FreeingBufferRecycler.INSTANCE, false, receivedSize);

				inputChannel.onBuffer(buffer, bufferOrEvent.sequenceNumber, -1);

				return true;
			}
		}
		finally {
			if (releaseNettyBuffer) {
				bufferOrEvent.releaseBuffer();
			}
		}
	}

	private class AsyncErrorNotificationTask implements Runnable {

		private final Throwable error;

		public AsyncErrorNotificationTask(Throwable error) {
			this.error = error;
		}

		@Override
		public void run() {
			notifyAllChannelsOfErrorAndClose(error);
		}
	}

	/**
	 * A buffer availability listener, which subscribes/unsubscribes the NIO
	 * read event.
	 *
	 * <p>If no buffer is available, the channel read event will be unsubscribed
	 * until one becomes available again.
	 *
	 * <p>After a buffer becomes available again, the buffer is handed over by
	 * the thread calling {@link #notifyBufferAvailable(Buffer)} to the network I/O
	 * thread, which then continues the processing of the staged buffer.
	 *
	 * 当BufferProvider没有buffer能提供时,需要监听什么时候可以提供buffer
	 */
	private class BufferListenerTask implements BufferListener, Runnable {

		private final AtomicReference<Buffer> availableBuffer = new AtomicReference<Buffer>();//可用的buffer

		private NettyMessage.BufferResponse stagedBufferResponse;//服务器返回的对象

		private boolean waitForBuffer(BufferProvider bufferProvider, NettyMessage.BufferResponse bufferResponse) {

			stagedBufferResponse = bufferResponse;

			if (bufferProvider.addBufferListener(this)) {
				if (ctx.channel().config().isAutoRead()) {
					ctx.channel().config().setAutoRead(false);
				}

				return true;
			}
			else {
				stagedBufferResponse = null;

				return false;
			}
		}

		private boolean hasStagedBufferOrEvent() {
			return stagedBufferResponse != null;
		}

		public void notifyBufferDestroyed() {
			// The buffer pool has been destroyed
			stagedBufferResponse = null;

			if (stagedMessages.isEmpty()) {
				ctx.channel().config().setAutoRead(true);
				ctx.channel().read();
			}
			else {
				ctx.channel().eventLoop().execute(stagedMessagesHandler);
			}
		}

		// Called by the recycling thread (not network I/O thread)
		//说明有buffer可用了
		@Override
		public boolean notifyBufferAvailable(Buffer buffer) {
			boolean success = false;

			try {
				if (availableBuffer.compareAndSet(null, buffer)) {
					ctx.channel().eventLoop().execute(this);

					success = true;
				}
				else {
					throw new IllegalStateException("Received a buffer notification, " +
							" but the previous one has not been handled yet.");
				}
			}
			catch (Throwable t) {
				ctx.channel().eventLoop().execute(new AsyncErrorNotificationTask(t));
			}
			finally {
				if (!success) {
					if (buffer != null) {
						buffer.recycleBuffer();
					}
				}
			}

			return false;
		}

		/**
		 * Continues the decoding of a staged buffer after a buffer has become available again.
		 *
		 * <p>This task is executed by the network I/O thread.
		 */
		@Override
		public void run() {
			boolean success = false;

			Buffer buffer = null;

			try {
				if ((buffer = availableBuffer.getAndSet(null)) == null) {//获取可用的buffer
					throw new IllegalStateException("Running buffer availability task w/o a buffer.");
				}

				ByteBuf nettyBuffer = stagedBufferResponse.getNettyBuffer();
				nettyBuffer.readBytes(buffer.asByteBuf(), nettyBuffer.readableBytes());//将数据读取到可用的buffer中
				stagedBufferResponse.releaseBuffer();

				RemoteInputChannel inputChannel = inputChannels.get(stagedBufferResponse.receiverId);

				if (inputChannel != null) {
					inputChannel.onBuffer(buffer, stagedBufferResponse.sequenceNumber, -1);

					success = true;
				}
				else {
					cancelRequestFor(stagedBufferResponse.receiverId);
				}

				stagedBufferResponse = null;

				if (stagedMessages.isEmpty()) {
					ctx.channel().config().setAutoRead(true);
					ctx.channel().read();
				}
				else {
					ctx.channel().eventLoop().execute(stagedMessagesHandler);
				}
			}
			catch (Throwable t) {
				notifyAllChannelsOfErrorAndClose(t);
			}
			finally {
				if (!success) {
					if (buffer != null) {
						buffer.recycleBuffer();
					}
				}
			}
		}
	}

	public class StagedMessagesHandlerTask implements Runnable {

		@Override
		public void run() {
			try {
				Object msg;
				while ((msg = stagedMessages.poll()) != null) {//不断去解码数据
					if (!decodeMsg(msg, true)) {
						return;
					}
				}

				ctx.channel().config().setAutoRead(true);
				ctx.channel().read();
			}
			catch (Throwable t) {
				notifyAllChannelsOfErrorAndClose(t);
			}
		}
	}
}
