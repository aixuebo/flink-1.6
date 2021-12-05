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

package org.apache.flink.runtime.zookeeper;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.runtime.util.EnvironmentInformation;

import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerMain;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * Simple wrapper for ZooKeeper's {@link QuorumPeer}, which reads a ZooKeeper config file and writes
 * the required 'myid' file before starting the peer.
 *
 * 启动zookeeper,假设有3个节点则需要启动3次。分别传入不同的peerId
 *
 * 启动后多个server节点争夺leader节点,因此zookeeper就可以用来服务了
 */
public class FlinkZooKeeperQuorumPeer {

	/** ZooKeeper default client port. */
	public static final int DEFAULT_ZOOKEEPER_CLIENT_PORT = 2181;

	/** ZooKeeper default init limit. */
	public static final int DEFAULT_ZOOKEEPER_INIT_LIMIT = 10;

	/** ZooKeeper default sync limit. */
	public static final int DEFAULT_ZOOKEEPER_SYNC_LIMIT = 5;

	/** ZooKeeper default peer port. */
	public static final int DEFAULT_ZOOKEEPER_PEER_PORT = 2888;

	/** ZooKeeper default leader port. */
	public static final int DEFAULT_ZOOKEEPER_LEADER_PORT = 3888;

	private static final Logger LOG = LoggerFactory.getLogger(FlinkZooKeeperQuorumPeer.class);

	// ------------------------------------------------------------------------

	public static void main(String[] args) {
		try {
			// startup checks and logging
			EnvironmentInformation.logEnvironmentInfo(LOG, "ZooKeeper Quorum Peer", args);
			
			final ParameterTool params = ParameterTool.fromArgs(args);
			final String zkConfigFile = params.getRequired("zkConfigFile");//zookeeper的配置信息
			final int peerId = params.getInt("peerId");

			// Run quorum peer
			runFlinkZkQuorumPeer(zkConfigFile, peerId);
		}
		catch (Throwable t) {
			LOG.error("Error running ZooKeeper quorum peer: " + t.getMessage(), t);
			System.exit(-1);
		}
	}

	// ------------------------------------------------------------------------

	/**
	 * Runs a ZooKeeper {@link QuorumPeer} if further peers are configured or a single
	 * {@link ZooKeeperServer} if no further peers are configured.
	 *
	 * @param zkConfigFile ZooKeeper config file 'zoo.cfg'
	 * @param peerId       ID for the 'myid' file
	 */
	public static void runFlinkZkQuorumPeer(String zkConfigFile, int peerId) throws Exception {

		Properties zkProps = new Properties();

		try (InputStream inStream = new FileInputStream(new File(zkConfigFile))) {//读取zookeeper的配置信息
			zkProps.load(inStream);
		}

		LOG.info("Configuration: " + zkProps);

		// Set defaults for required properties
		setRequiredProperties(zkProps);//设置zkProps额外的信息

		// Write peer id to myid file
		writeMyIdToDataDir(zkProps, peerId);//输出id到文件中

		// The myid file needs to be written before creating the instance. Otherwise, this
		// will fail.
		QuorumPeerConfig conf = new QuorumPeerConfig();
		conf.parseProperties(zkProps);

		if (conf.isDistributed()) {
			// Run quorum peer
			LOG.info("Running distributed ZooKeeper quorum peer (total peers: {}).",
					conf.getServers().size());

			QuorumPeerMain qp = new QuorumPeerMain();//多个zookeeper 的server争夺leader节点
			qp.runFromConfig(conf);
		}
		else {
			// Run standalone
			LOG.info("Running standalone ZooKeeper quorum peer.");

			ZooKeeperServerMain zk = new ZooKeeperServerMain();
			ServerConfig sc = new ServerConfig();
			sc.readFrom(conf);
			zk.runFromConfig(sc);
		}
	}

	/**
	 * Sets required properties to reasonable defaults and logs it.
	 * 设置zkProps额外的信息
	 */
	private static void setRequiredProperties(Properties zkProps) {
		// Set default client port
		if (zkProps.getProperty("clientPort") == null) {
			zkProps.setProperty("clientPort", String.valueOf(DEFAULT_ZOOKEEPER_CLIENT_PORT));

			LOG.warn("No 'clientPort' configured. Set to '{}'.", DEFAULT_ZOOKEEPER_CLIENT_PORT);
		}

		// Set default init limit
		if (zkProps.getProperty("initLimit") == null) {
			zkProps.setProperty("initLimit", String.valueOf(DEFAULT_ZOOKEEPER_INIT_LIMIT));

			LOG.warn("No 'initLimit' configured. Set to '{}'.", DEFAULT_ZOOKEEPER_INIT_LIMIT);
		}

		// Set default sync limit
		if (zkProps.getProperty("syncLimit") == null) {
			zkProps.setProperty("syncLimit", String.valueOf(DEFAULT_ZOOKEEPER_SYNC_LIMIT));

			LOG.warn("No 'syncLimit' configured. Set to '{}'.", DEFAULT_ZOOKEEPER_SYNC_LIMIT);
		}

		// Set default data dir
		if (zkProps.getProperty("dataDir") == null) {
			String dataDir = String.format("%s/%s/zookeeper",
					System.getProperty("java.io.tmpdir"), UUID.randomUUID().toString());

			zkProps.setProperty("dataDir", dataDir);

			LOG.warn("No 'dataDir' configured. Set to '{}'.", dataDir);
		}

		int peerPort = DEFAULT_ZOOKEEPER_PEER_PORT;
		int leaderPort = DEFAULT_ZOOKEEPER_LEADER_PORT;

		// Set peer and leader ports if none given, because ZooKeeper complains if multiple
		// servers are configured, but no ports are given.
		//设置每一个ip 对应的 peerport和leaderPort
		for (Map.Entry<Object, Object> entry : zkProps.entrySet()) {
			String key = (String) entry.getKey();

			if (entry.getKey().toString().startsWith("server.")) {
				String value = (String) entry.getValue();
				String[] parts = value.split(":");

				if (parts.length == 1) {
					String address = String.format("%s:%d:%d", parts[0], peerPort, leaderPort);
					zkProps.setProperty(key, address);
					LOG.info("Set peer and leader port of '{}': '{}' => '{}'.",
							key, value, address);
				}
				else if (parts.length == 2) {
					String address = String.format("%s:%d:%d",
							parts[0], Integer.valueOf(parts[1]), leaderPort);
					zkProps.setProperty(key, address);
					LOG.info("Set peer port of '{}': '{}' => '{}'.", key, value, address);
				}
			}
		}
	}

	/**
	 * Write 'myid' file to the 'dataDir' in the given ZooKeeper configuration.
	 *
	 * <blockquote>
	 * Every machine that is part of the ZooKeeper ensemble should know about every other machine in
	 * the ensemble. You accomplish this with the series of lines of the form
	 * server.id=host:port:port. The parameters host and port are straightforward. You attribute the
	 * server id to each machine by creating a file named myid, one for each server, which resides
	 * in that server's data directory, as specified by the configuration file parameter dataDir.
	 * </blockquote>
	 *
	 * @param zkProps ZooKeeper configuration.
	 * @param id      The ID of this {@link QuorumPeer}.
	 * @throws IllegalConfigurationException Thrown, if 'dataDir' property not set in given
	 *                                       ZooKeeper properties.
	 * @throws IOException                   Thrown, if 'dataDir' does not exist and cannot be
	 *                                       created.
	 * @see <a href="http://zookeeper.apache.org/doc/r3.4.6/zookeeperAdmin.html">
	 * ZooKeeper Administrator's Guide</a>
	 *
	 * 在dataDir目录下,创建文件myid,设置id
	 */
	private static void writeMyIdToDataDir(Properties zkProps, int id) throws IOException {

		// Check dataDir and create if necessary
		if (zkProps.getProperty("dataDir") == null) {
			throw new IllegalConfigurationException("No dataDir configured.");
		}

		File dataDir = new File(zkProps.getProperty("dataDir"));//数据存储本地的目录

		if (!dataDir.isDirectory() && !dataDir.mkdirs()) {
			throw new IOException("Cannot create dataDir '" + dataDir + "'.");
		}

		dataDir.deleteOnExit();

		LOG.info("Writing {} to myid file in 'dataDir'.", id);
		
		// Write myid to file. We use a File Writer, because that properly propagates errors,
		// while the PrintWriter swallows errors
		try (FileWriter writer = new FileWriter(new File(dataDir, "myid"))) {
			writer.write(String.valueOf(id));
		}
	}
}
