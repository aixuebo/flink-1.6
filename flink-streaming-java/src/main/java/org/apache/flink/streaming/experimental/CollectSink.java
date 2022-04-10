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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;

/**
 * A specialized data sink to be used by DataStreamUtils.collect().
 *
 * <p>This experimental class is relocated from flink-streaming-contrib. Please see package-info.java
 * for more information.
 * 本类是客户端:将value转换成二进制,输出到ip+port的服务器
 */
@Experimental
public class CollectSink<IN> extends RichSinkFunction<IN> {

	private static final long serialVersionUID = 1L;

	//socket
	private final InetAddress hostIp;
	private final int port;
	private final TypeSerializer<IN> serializer;//如何序列化对象

	private transient Socket client;//socket客户端
	private transient OutputStream outputStream;
	private transient DataOutputViewStreamWrapper streamWriter;//client的输出流

	/**
	 * Creates a CollectSink that will send the data to the specified host.
	 *
	 * @param hostIp IP address of the Socket server.
	 * @param port Port of the Socket server.
	 * @param serializer A serializer for the data.
	 */
	public CollectSink(InetAddress hostIp, int port, TypeSerializer<IN> serializer) {
		this.hostIp = hostIp;
		this.port = port;
		this.serializer = serializer;
	}

	//将value转换成二进制,输出到ip+port
	@Override
	public void invoke(IN value, Context context) throws Exception {
		try {
			serializer.serialize(value, streamWriter);
		}
		catch (Exception e) {
			throw new IOException("Error sending data back to client (" + hostIp.toString() + ":" + port + ')', e);
		}
	}

	/**
	 * Initialize the connection with the Socket in the server.
	 * @param parameters Configuration.
	 * 建设输出流
	 */
	@Override
	public void open(Configuration parameters) throws Exception {
		try {
			client = new Socket(hostIp, port);
			outputStream = client.getOutputStream();
			streamWriter = new DataOutputViewStreamWrapper(outputStream);
		}
		catch (IOException e) {
			throw new IOException("Cannot connect to the client to send back the stream", e);
		}
	}

	/**
	 * Closes the connection with the Socket server.
	 */
	@Override
	public void close() throws Exception {
		try {
			if (outputStream != null) {
				outputStream.flush();
				outputStream.close();
			}

			// first regular attempt to cleanly close. Failing that will escalate
			if (client != null) {
				client.close();
			}
		}
		catch (Exception e) {
			throw new IOException("Error while closing connection that streams data back to client at "
					+ hostIp.toString() + ":" + port, e);
		}
		finally {
			// if we failed prior to closing the client, close it
			if (client != null) {
				try {
					client.close();
				}
				catch (Throwable t) {
					// best effort to close, we do not care about an exception here any more
				}
			}
		}
	}
}
