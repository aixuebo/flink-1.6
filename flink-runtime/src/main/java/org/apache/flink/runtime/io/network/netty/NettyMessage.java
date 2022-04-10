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

import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.util.ExceptionUtils;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufAllocator;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufInputStream;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufOutputStream;
import org.apache.flink.shaded.netty4.io.netty.buffer.CompositeByteBuf;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelOutboundHandlerAdapter;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelPromise;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.LengthFieldBasedFrameDecoder;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ProtocolException;
import java.nio.ByteBuffer;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A simple and generic interface to serialize messages to Netty's buffer space.
 *
 * <p>This class must be public as long as we are using a Netty version prior to 4.0.45. Please check FLINK-7845 for
 * more information.
 * 表示一条信息
 */
public abstract class NettyMessage {

	// ------------------------------------------------------------------------
	// Note: Every NettyMessage subtype needs to have a public 0-argument
	// constructor in order to work with the generic deserializer.
	// ------------------------------------------------------------------------

	static final int FRAME_HEADER_LENGTH = 4 + 4 + 1; // frame length (4), magic number (4), msg ID (1) --表示NettyMessage的具体子类

	static final int MAGIC_NUMBER = 0xBADC0FFE;

	//将信息分配一个buffer，写入到该buffer中，返回内存的buffer
	//即将信息写入buffer,并且返回 --- 该buffer是由参数创建的
	abstract ByteBuf write(ByteBufAllocator allocator) throws Exception;

	// ------------------------------------------------------------------------

	/**
	 * Allocates a new (header and contents) buffer and adds some header information for the frame
	 * decoder.
	 *
	 * <p>Before sending the buffer, you must write the actual length after adding the contents as
	 * an integer to position <tt>0</tt>!
	 *
	 * @param allocator
	 * 		byte buffer allocator to use
	 * @param id
	 * 		{@link NettyMessage} subclass ID
	 *
	 * @return a newly allocated direct buffer with header data written for {@link
	 * NettyMessageDecoder}
	 * 分配一个buffer,里面存储格式:总字节+模+id+内容
	 */
	private static ByteBuf allocateBuffer(ByteBufAllocator allocator, byte id) {
		return allocateBuffer(allocator, id, -1);
	}

	/**
	 * Allocates a new (header and contents) buffer and adds some header information for the frame
	 * decoder.
	 *
	 * <p>If the <tt>contentLength</tt> is unknown, you must write the actual length after adding
	 * the contents as an integer to position <tt>0</tt>!
	 *
	 * @param allocator
	 * 		byte buffer allocator to use
	 * @param id
	 * 		{@link NettyMessage} subclass ID
	 * @param contentLength
	 * 		content length (or <tt>-1</tt> if unknown)
	 *
	 * @return a newly allocated direct buffer with header data written for {@link
	 * NettyMessageDecoder}
	 * 分配一个buffer,里面存储格式:总字节+模+id+内容
	 * 其中如果contentLength>0，表示已经知道内容长度,则不需要添加完内容后，去头部更新总字节数。
	 * 如果contentLength = -1,表示不知道内容长度，因此添加完成内容后，要去头部更新总字节数
	 */
	private static ByteBuf allocateBuffer(ByteBufAllocator allocator, byte id, int contentLength) {
		return allocateBuffer(allocator, id, 0, contentLength, true);
	}

	/**
	 * Allocates a new buffer and adds some header information for the frame decoder.
	 *
	 * <p>If the <tt>contentLength</tt> is unknown, you must write the actual length after adding
	 * the contents as an integer to position <tt>0</tt>!
	 *
	 * @param allocator
	 * 		byte buffer allocator to use
	 * @param id
	 * 		{@link NettyMessage} subclass ID
	 * @param messageHeaderLength
	 * 		additional header length that should be part of the allocated buffer and is written
	 * 		outside of this method
	 * @param contentLength
	 * 		content length (or <tt>-1</tt> if unknown)
	 * @param allocateForContent
	 * 		whether to make room for the actual content in the buffer (<tt>true</tt>) or whether to
	 * 		only return a buffer with the header information (<tt>false</tt>) ,true表示要存储内容，false表示只存储头文件
	 *
	 * @return a newly allocated direct buffer with header data written for {@link
	 * NettyMessageDecoder}
	 * 分配一个buffer,里面存储格式:总字节+模+id+额外头文件+内容
	 * 其中如果contentLength>0，表示已经知道内容长度,则不需要添加完内容后，去头部更新总字节数。
	 * 如果contentLength = -1,表示不知道内容长度，因此添加完成内容后，要去头部更新总字节数
	 */
	private static ByteBuf allocateBuffer(
			ByteBufAllocator allocator,
			byte id,
			int messageHeaderLength,//额外头文件内容
			int contentLength,
			boolean allocateForContent) {
		checkArgument(contentLength <= Integer.MAX_VALUE - FRAME_HEADER_LENGTH);

		final ByteBuf buffer;
		if (!allocateForContent) {//说明只要头文件，不需要内容，因此分配空间是 模+id+额外头文件
			buffer = allocator.directBuffer(FRAME_HEADER_LENGTH + messageHeaderLength);
		} else if (contentLength != -1) {//说明已经知道了文件内容大小,因此分配空间为 模+id+额外头文件+文件内容
			buffer = allocator.directBuffer(FRAME_HEADER_LENGTH + messageHeaderLength + contentLength);
		} else {//说明不知道文件内容大小,分配空间
			// content length unknown -> start with the default initial size (rather than FRAME_HEADER_LENGTH only):
			buffer = allocator.directBuffer();
		}
		buffer.writeInt(FRAME_HEADER_LENGTH + messageHeaderLength + contentLength); // may be updated later, e.g. if contentLength == -1,如果是-1的时候,要重新分配字节大小
		buffer.writeInt(MAGIC_NUMBER);
		buffer.writeByte(id);

		return buffer;
	}

	// ------------------------------------------------------------------------
	// Generic NettyMessage encoder and decoder
	// ------------------------------------------------------------------------

	//如何对外输出信息 --- 比如客户端发送给服务器,再客户端侧,就要继承ChannelOutboundHandlerAdapter,用于实现write方法
	//将NettyMessage转换成字节数组,并且发送出去
	@ChannelHandler.Sharable
	static class NettyMessageEncoder extends ChannelOutboundHandlerAdapter {

		@Override
		public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
			if (msg instanceof NettyMessage) {

				ByteBuf serialized = null;

				try {
					serialized = ((NettyMessage) msg).write(ctx.alloc());//将msg信息输出到内存的buffer中,并且返回包含信息的buffer
				}
				catch (Throwable t) {
					throw new IOException("Error while serializing message: " + msg, t);
				}
				finally {
					if (serialized != null) {
						ctx.write(serialized, promise);//向上继续输出buffer
					}
				}
			}
			else {
				ctx.write(msg, promise);//直接向上输出msg对象
			}
		}
	}

	/**
	 * Message decoder based on netty's {@link LengthFieldBasedFrameDecoder} but avoiding the
	 * additional memory copy inside {@link #extractFrame(ChannelHandlerContext, ByteBuf, int, int)}
	 * since we completely decode the {@link ByteBuf} inside {@link #decode(ChannelHandlerContext,
	 * ByteBuf)} and will not re-use it afterwards.
	 *
	 * <p>The frame-length encoder will be based on this transmission scheme created by {@link NettyMessage#allocateBuffer(ByteBufAllocator, byte, int)}:
	 * <pre>
	 * +------------------+------------------+--------++----------------+
	 * | FRAME LENGTH (4) | MAGIC NUMBER (4) | ID (1) || CUSTOM MESSAGE |
	 * +------------------+------------------+--------++----------------+
	 * 固定长度，MAGIC，message类型,自定义内容   格式:长度+魔+NettyMessage类型的id+内容
	 * </pre>
	 *
	 * 他是一个ChannelInboundHandlerAdapter，读取的是ByteBuf
	 * 固定长度的字节数组
	 *
	 * 即如何将ByteBuf转换成NettyMessage对象
	 */
	static class NettyMessageDecoder extends LengthFieldBasedFrameDecoder {
		private final boolean restoreOldNettyBehaviour;

		/**
		 * Creates a new message decoded with the required frame properties.
		 *
		 * @param restoreOldNettyBehaviour
		 * 		restore Netty 4.0.27 code in {@link LengthFieldBasedFrameDecoder#extractFrame} to
		 * 		copy instead of slicing the buffer
		 */
		NettyMessageDecoder(boolean restoreOldNettyBehaviour) {
			super(Integer.MAX_VALUE, 0, 4, -4, 4);
			this.restoreOldNettyBehaviour = restoreOldNettyBehaviour;
		}

		@Override
		protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
			ByteBuf msg = (ByteBuf) super.decode(ctx, in);//读取第一个int值,获取buffer字节长度,填充buffer
			if (msg == null) {
				return null;
			}

			try {
				int magicNumber = msg.readInt();//获取魔

				if (magicNumber != MAGIC_NUMBER) {
					throw new IllegalStateException(
						"Network stream corrupted: received incorrect magic number.");
				}

				byte msgId = msg.readByte();//id类型

				//继续反序列化成对象NettyMessage
				final NettyMessage decodedMsg;
				switch (msgId) {
					case BufferResponse.ID:
						decodedMsg = BufferResponse.readFrom(msg);
						break;
					case PartitionRequest.ID:
						decodedMsg = PartitionRequest.readFrom(msg);
						break;
					case TaskEventRequest.ID:
						decodedMsg = TaskEventRequest.readFrom(msg, getClass().getClassLoader());
						break;
					case ErrorResponse.ID:
						decodedMsg = ErrorResponse.readFrom(msg);
						break;
					case CancelPartitionRequest.ID:
						decodedMsg = CancelPartitionRequest.readFrom(msg);
						break;
					case CloseRequest.ID:
						decodedMsg = CloseRequest.readFrom(msg);
						break;
					case AddCredit.ID:
						decodedMsg = AddCredit.readFrom(msg);
						break;
					default:
						throw new ProtocolException(
							"Received unknown message from producer: " + msg);
				}

				return decodedMsg;
			} finally {
				// ByteToMessageDecoder cleanup (only the BufferResponse holds on to the decoded
				// msg but already retain()s the buffer once)
				msg.release();
			}
		}

		@Override
		protected ByteBuf extractFrame(ChannelHandlerContext ctx, ByteBuf buffer, int index, int length) {
			if (restoreOldNettyBehaviour) {
				/*
				 * For non-credit based code paths with Netty >= 4.0.28.Final:
				 * These versions contain an improvement by Netty, which slices a Netty buffer
				 * instead of doing a memory copy [1] in the
				 * LengthFieldBasedFrameDecoder. In some situations, this
				 * interacts badly with our Netty pipeline leading to OutOfMemory
				 * errors.
				 *
				 * [1] https://github.com/netty/netty/issues/3704
				 *
				 * TODO: remove along with the non-credit based fallback protocol
				 */
				ByteBuf frame = ctx.alloc().buffer(length);
				frame.writeBytes(buffer, index, length);
				return frame;
			} else {
				return super.extractFrame(ctx, buffer, index, length);
			}
		}
	}

	// ------------------------------------------------------------------------
	// Server responses
	// ------------------------------------------------------------------------

	static class BufferResponse extends NettyMessage {

		private static final byte ID = 0;

		final ByteBuf buffer;//具体内容

		final InputChannelID receiverId;//16个字节 --- 发送方是哪个流ID

		final int sequenceNumber;//buffer是tcp第几个序号流

		final int backlog;

		final boolean isBuffer;

		private BufferResponse(
				ByteBuf buffer,
				boolean isBuffer,
				int sequenceNumber,
				InputChannelID receiverId,
				int backlog) {
			this.buffer = checkNotNull(buffer);
			this.isBuffer = isBuffer;
			this.sequenceNumber = sequenceNumber;
			this.receiverId = checkNotNull(receiverId);
			this.backlog = backlog;
		}

		BufferResponse(
				Buffer buffer,
				int sequenceNumber,
				InputChannelID receiverId,
				int backlog) {
			this.buffer = checkNotNull(buffer).asByteBuf();
			this.isBuffer = buffer.isBuffer();
			this.sequenceNumber = sequenceNumber;
			this.receiverId = checkNotNull(receiverId);
			this.backlog = backlog;
		}

		boolean isBuffer() {
			return isBuffer;
		}

		ByteBuf getNettyBuffer() {
			return buffer;
		}

		void releaseBuffer() {
			buffer.release();
		}

		// --------------------------------------------------------------------
		// Serialization
		// --------------------------------------------------------------------

		@Override
		ByteBuf write(ByteBufAllocator allocator) throws IOException {
			// receiver ID (16), sequence number (4), backlog (4), isBuffer (1), buffer size (4)
			final int messageHeaderLength = 16 + 4 + 4 + 1 + 4;

			ByteBuf headerBuf = null;
			try {
				if (buffer instanceof Buffer) {
					// in order to forward the buffer to netty, it needs an allocator set
					((Buffer) buffer).setAllocator(allocator);
				}

				// only allocate header buffer - we will combine it with the data buffer below
				headerBuf = allocateBuffer(allocator, ID, messageHeaderLength, buffer.readableBytes(), false);//分配已知道内容的头文件
				//此时已经追加了总长度、模、id到headerBuf中
				receiverId.writeTo(headerBuf);//16个字节
				headerBuf.writeInt(sequenceNumber);
				headerBuf.writeInt(backlog);
				headerBuf.writeBoolean(isBuffer);
				headerBuf.writeInt(buffer.readableBytes());

				CompositeByteBuf composityBuf = allocator.compositeDirectBuffer();
				composityBuf.addComponent(headerBuf);
				composityBuf.addComponent(buffer);
				// update writer index since we have data written to the components:
				composityBuf.writerIndex(headerBuf.writerIndex() + buffer.writerIndex());
				return composityBuf;
			}
			catch (Throwable t) {
				if (headerBuf != null) {
					headerBuf.release();
				}
				buffer.release();

				ExceptionUtils.rethrowIOException(t);
				return null; // silence the compiler
			}
		}

		static BufferResponse readFrom(ByteBuf buffer) {
			InputChannelID receiverId = InputChannelID.fromByteBuf(buffer);
			int sequenceNumber = buffer.readInt();
			int backlog = buffer.readInt();
			boolean isBuffer = buffer.readBoolean();
			int size = buffer.readInt();

			ByteBuf retainedSlice = buffer.readSlice(size).retain();
			return new BufferResponse(retainedSlice, isBuffer, sequenceNumber, receiverId, backlog);
		}
	}

	static class ErrorResponse extends NettyMessage {

		private static final byte ID = 1;

		final Throwable cause;

		@Nullable
		final InputChannelID receiverId;

		ErrorResponse(Throwable cause) {
			this.cause = checkNotNull(cause);
			this.receiverId = null;
		}

		ErrorResponse(Throwable cause, InputChannelID receiverId) {
			this.cause = checkNotNull(cause);
			this.receiverId = receiverId;
		}

		boolean isFatalError() {
			return receiverId == null;
		}

		@Override
		ByteBuf write(ByteBufAllocator allocator) throws IOException {
			final ByteBuf result = allocateBuffer(allocator, ID);

			try (ObjectOutputStream oos = new ObjectOutputStream(new ByteBufOutputStream(result))) {
				oos.writeObject(cause);

				if (receiverId != null) {
					result.writeBoolean(true);
					receiverId.writeTo(result);
				} else {
					result.writeBoolean(false);
				}

				// Update frame length...
				result.setInt(0, result.readableBytes());
				return result;
			}
			catch (Throwable t) {
				result.release();

				if (t instanceof IOException) {
					throw (IOException) t;
				} else {
					throw new IOException(t);
				}
			}
		}

		static ErrorResponse readFrom(ByteBuf buffer) throws Exception {
			try (ObjectInputStream ois = new ObjectInputStream(new ByteBufInputStream(buffer))) {
				Object obj = ois.readObject();

				if (!(obj instanceof Throwable)) {
					throw new ClassCastException("Read object expected to be of type Throwable, " +
							"actual type is " + obj.getClass() + ".");
				} else {
					if (buffer.readBoolean()) {
						InputChannelID receiverId = InputChannelID.fromByteBuf(buffer);
						return new ErrorResponse((Throwable) obj, receiverId);
					} else {
						return new ErrorResponse((Throwable) obj);
					}
				}
			}
		}
	}

	// ------------------------------------------------------------------------
	// Client requests 客户端请求去拉去某一个partition分区结果数据
	// ------------------------------------------------------------------------

	static class PartitionRequest extends NettyMessage {

		private static final byte ID = 2;

		final ResultPartitionID partitionId;//16字节 获取partition的ID

		final int queueIndex;//获取第几个子partition信息

		final InputChannelID receiverId;//16字节 返回的信息拉回来后，存储在哪个流里? 暂时还不确定

		final int credit;

		PartitionRequest(ResultPartitionID partitionId, int queueIndex, InputChannelID receiverId, int credit) {
			this.partitionId = checkNotNull(partitionId);
			this.queueIndex = queueIndex;
			this.receiverId = checkNotNull(receiverId);
			this.credit = credit;
		}

		@Override
		ByteBuf write(ByteBufAllocator allocator) throws IOException {
			ByteBuf result = null;

			try {
				result = allocateBuffer(allocator, ID, 16 + 16 + 4 + 16 + 4);//设置已经固定长度的字节内容
				//此时已经追加了总长度、模、id到result中
				partitionId.getPartitionId().writeTo(result);//16个字节
				partitionId.getProducerId().writeTo(result);//16个字节
				result.writeInt(queueIndex);//4个字节
				receiverId.writeTo(result);//16个字节
				result.writeInt(credit);//4个字节

				return result;
			}
			catch (Throwable t) {
				if (result != null) {
					result.release();
				}

				throw new IOException(t);
			}
		}

		static PartitionRequest readFrom(ByteBuf buffer) {
			ResultPartitionID partitionId =
				new ResultPartitionID(
					IntermediateResultPartitionID.fromByteBuf(buffer),
					ExecutionAttemptID.fromByteBuf(buffer));
			int queueIndex = buffer.readInt();
			InputChannelID receiverId = InputChannelID.fromByteBuf(buffer);
			int credit = buffer.readInt();

			return new PartitionRequest(partitionId, queueIndex, receiverId, credit);
		}

		@Override
		public String toString() {
			return String.format("PartitionRequest(%s:%d:%d)", partitionId, queueIndex, credit);
		}
	}

	static class TaskEventRequest extends NettyMessage {

		private static final byte ID = 3;

		final TaskEvent event;//需要被序列化

		final InputChannelID receiverId;

		final ResultPartitionID partitionId;

		TaskEventRequest(TaskEvent event, ResultPartitionID partitionId, InputChannelID receiverId) {
			this.event = checkNotNull(event);
			this.receiverId = checkNotNull(receiverId);
			this.partitionId = checkNotNull(partitionId);
		}

		@Override
		ByteBuf write(ByteBufAllocator allocator) throws IOException {
			ByteBuf result = null;

			try {
				// TODO Directly serialize to Netty's buffer
				ByteBuffer serializedEvent = EventSerializer.toSerializedEvent(event);

				result = allocateBuffer(allocator, ID, 4 + serializedEvent.remaining() + 16 + 16 + 16);//设置已经固定长度的字节内容
				//此时已经追加了总长度、模、id到result中
				result.writeInt(serializedEvent.remaining());//写入4个字节--代表长度
				result.writeBytes(serializedEvent);//写入具体的内容

				partitionId.getPartitionId().writeTo(result);//写入16个字节
				partitionId.getProducerId().writeTo(result);//写入16个字节

				receiverId.writeTo(result);//写入16个字节

				return result;
			}
			catch (Throwable t) {
				if (result != null) {
					result.release();
				}

				throw new IOException(t);
			}
		}

		static TaskEventRequest readFrom(ByteBuf buffer, ClassLoader classLoader) throws IOException {
			// directly deserialize fromNetty's buffer
			int length = buffer.readInt();
			ByteBuffer serializedEvent = buffer.nioBuffer(buffer.readerIndex(), length);
			// assume this event's content is read from the ByteBuf (positions are not shared!)
			buffer.readerIndex(buffer.readerIndex() + length);

			TaskEvent event =
				(TaskEvent) EventSerializer.fromSerializedEvent(serializedEvent, classLoader);

			ResultPartitionID partitionId =
				new ResultPartitionID(
					IntermediateResultPartitionID.fromByteBuf(buffer),
					ExecutionAttemptID.fromByteBuf(buffer));

			InputChannelID receiverId = InputChannelID.fromByteBuf(buffer);

			return new TaskEventRequest(event, partitionId, receiverId);
		}
	}

	/**
	 * Cancels the partition request of the {@link InputChannel} identified by
	 * {@link InputChannelID}.
	 *
	 * <p>There is a 1:1 mapping between the input channel and partition per physical channel.
	 * Therefore, the {@link InputChannelID} instance is enough to identify which request to cancel.
	 */
	static class CancelPartitionRequest extends NettyMessage {

		private static final byte ID = 4;

		final InputChannelID receiverId;//本地的该流要被取消，通知服务器也一并删除了吧

		CancelPartitionRequest(InputChannelID receiverId) {
			this.receiverId = checkNotNull(receiverId);
		}

		@Override
		ByteBuf write(ByteBufAllocator allocator) throws Exception {
			ByteBuf result = null;

			try {
				result = allocateBuffer(allocator, ID, 16);//设置已经固定长度的字节内容
				//此时已经追加了总长度、模、id到result中
				receiverId.writeTo(result);//写入16个字节
			}
			catch (Throwable t) {
				if (result != null) {
					result.release();
				}

				throw new IOException(t);
			}

			return result;
		}

		static CancelPartitionRequest readFrom(ByteBuf buffer) throws Exception {
			return new CancelPartitionRequest(InputChannelID.fromByteBuf(buffer));
		}
	}

	static class CloseRequest extends NettyMessage {

		private static final byte ID = 5;

		CloseRequest() {
		}

		@Override
		ByteBuf write(ByteBufAllocator allocator) throws Exception {
			return allocateBuffer(allocator, ID, 0);//此时已经追加了总长度、模、id到buffer中
		}

		static CloseRequest readFrom(@SuppressWarnings("unused") ByteBuf buffer) throws Exception {
			return new CloseRequest();
		}
	}

	/**
	 * Incremental credit announcement from the client to the server.
	 */
	static class AddCredit extends NettyMessage {

		private static final byte ID = 6;

		final ResultPartitionID partitionId;

		final int credit;

		final InputChannelID receiverId;

		AddCredit(ResultPartitionID partitionId, int credit, InputChannelID receiverId) {
			checkArgument(credit > 0, "The announced credit should be greater than 0");

			this.partitionId = partitionId;
			this.credit = credit;
			this.receiverId = receiverId;
		}

		@Override
		ByteBuf write(ByteBufAllocator allocator) throws IOException {
			ByteBuf result = null;

			try {
				result = allocateBuffer(allocator, ID, 16 + 16 + 4 + 16);//设置已经固定长度的字节内容
				//此时已经追加了总长度、模、id到result中
				partitionId.getPartitionId().writeTo(result);//写入16个字节
				partitionId.getProducerId().writeTo(result);//写入16个字节
				result.writeInt(credit);//写入4个字节
				receiverId.writeTo(result);//写入16个字节

				return result;
			}
			catch (Throwable t) {
				if (result != null) {
					result.release();
				}

				throw new IOException(t);
			}
		}

		static AddCredit readFrom(ByteBuf buffer) {
			ResultPartitionID partitionId =
				new ResultPartitionID(
					IntermediateResultPartitionID.fromByteBuf(buffer),
					ExecutionAttemptID.fromByteBuf(buffer));
			int credit = buffer.readInt();
			InputChannelID receiverId = InputChannelID.fromByteBuf(buffer);

			return new AddCredit(partitionId, credit, receiverId);
		}

		@Override
		public String toString() {
			return String.format("AddCredit(%s : %d)", receiverId, credit);
		}
	}
}
