/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.experimental;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * An iterator that returns the data from a socket stream.
 *
 * <p>The iterator's constructor opens a server socket. In the first call to {@link #next()}
 * or {@link #hasNext()}, the iterator waits for a socket to connect, and starts receiving,
 * deserializing, and returning the data from that socket.
 *
 * <p>This experimental class is relocated from flink-streaming-contrib. Please see package-info.java
 * for more information.
 *
 * @param <T> The type of elements returned from the iterator.
 * 服务端---接受客户端请求中,获取字节数组,反序列化成对象
 */
@Experimental
public class SocketStreamIterator<T> implements Iterator<T> {

	/** Server socket to listen at. */
	private final ServerSocket socket;//服务端

	/** Serializer to deserialize stream. */
	private final TypeSerializer<T> serializer;//如何反序列化对象

	/** Set by the same thread that reads it. */
	private DataInputViewStreamWrapper inStream;//读取客户端请求

	/** Next element, handover from hasNext() to next(). */
	private T next;//获取客户端发来的对象

	/** The socket for the specific stream. */
	private Socket connectedSocket;//接受流

	/** Async error, for example by the executor of the program that produces the stream. */
	private volatile Throwable error;

	/**
	 * Creates an iterator that returns the data from a socket stream with automatic port and bind address.
	 *
	 * @param serializer serializer used for deserializing incoming records
	 * @throws IOException thrown if socket cannot be opened
	 */
	public SocketStreamIterator(TypeSerializer<T> serializer) throws IOException {
		this(0, null, serializer);
	}

	/**
	 * Creates an iterator that returns the data from a socket stream with custom port and bind address.
	 *
	 * @param port port for the socket connection (0 means automatic port selection)
	 * @param address address for the socket connection
	 * @param serializer serializer used for deserializing incoming records
	 * @throws IOException thrown if socket cannot be opened
	 */
	public SocketStreamIterator(int port, InetAddress address, TypeSerializer<T> serializer) throws IOException {
		this.serializer = serializer;
		try {
			socket = new ServerSocket(port, 1, address);//创建服务
		}
		catch (IOException e) {
			throw new RuntimeException("Could not open socket to receive back stream results");
		}
	}

	// ------------------------------------------------------------------------
	//  properties
	// ------------------------------------------------------------------------

	/**
	 * Returns the port on which the iterator is getting the data. (Used internally.)
	 * @return The port
	 */
	public int getPort() {
		return socket.getLocalPort();
	}

	public InetAddress getBindAddress() {
		return socket.getInetAddress();
	}

	public void close() {
		if (connectedSocket != null) {
			try {
				connectedSocket.close();
			} catch (Throwable ignored) {}
		}

		try {
			socket.close();
		} catch (Throwable ignored) {}
	}

	// ------------------------------------------------------------------------
	//  iterator semantics
	// ------------------------------------------------------------------------

	/**
	 * Returns true if the DataStream has more elements.
	 * (Note: blocks if there will be more elements, but they are not available yet.)
	 * @return true if the DataStream has more elements
	 */
	@Override
	public boolean hasNext() {
		if (next == null) {
			try {
				next = readNextFromStream();//读取下一行数据对象
			} catch (Exception e) {
				throw new RuntimeException("Failed to receive next element: " + e.getMessage(), e);
			}
		}

		return next != null;
	}

	/**
	 * Returns the next element of the DataStream. (Blocks if it is not available yet.)
	 * @return The element
	 * @throws NoSuchElementException if the stream has already ended
	 */
	@Override
	public T next() {
		if (hasNext()) {
			T current = next;//获取下一行数据
			next = null;//这样就可以hasNext有数据了
			return current;
		} else {
			throw new NoSuchElementException();
		}
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

	private T readNextFromStream() throws Exception {
		try {
			if (inStream == null) {
				connectedSocket = socket.accept();//接受流
				inStream = new DataInputViewStreamWrapper(connectedSocket.getInputStream());
			}

			return serializer.deserialize(inStream);//反序列化成对象
		}
		catch (EOFException e) {
			try {
				connectedSocket.close();
			} catch (Throwable ignored) {}

			try {
				socket.close();
			} catch (Throwable ignored) {}

			return null;
		}
		catch (Exception e) {
			if (error == null) {
				throw e;
			}
			else {
				// throw the root cause error
				throw new Exception("Receiving stream failed: " + error.getMessage(), error);
			}
		}
	}

	// ------------------------------------------------------------------------
	//  errors
	// ------------------------------------------------------------------------

	public void notifyOfError(Throwable error) {
		if (error != null && this.error == null) {
			this.error = error;

			// this should wake up any blocking calls
			try {
				connectedSocket.close();
			} catch (Throwable ignored) {}
			try {
				socket.close();
			} catch (Throwable ignored) {}
		}
	}
}
