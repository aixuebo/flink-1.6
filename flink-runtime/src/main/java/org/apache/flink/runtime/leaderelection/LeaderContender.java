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

package org.apache.flink.runtime.leaderelection;

import java.util.UUID;

/**
 * Interface which has to be implemented to take part in the leader election process of the
 * {@link LeaderElectionService}.
 * leader竞争者 --- 子类实现如果是leader、不是leader要如何操作
 * 当选举服务显示该节点一定是leader的时候,则调用该类的实现类，起到通知作用。
 *
 * 选举出leader
 *
 * 该对象表示某一个竞选者
 *
 * 竞选者参与竞选,最核心要解决的是2个事儿:
 * 1.如果竞选成功了,如何做 void grantLeadership(UUID leaderSessionID);
 * 2.如果竞选失败了,如何做 void revokeLeadership();
 *
 * 辅助方法:
 * 1.提供竞选者自己的对外服务地址。
 * 2.提供竞选过程中,zookeeper等外部环境异常时,竟选择该如何做?
 */
public interface LeaderContender {

	/**
	 * Callback method which is called by the {@link LeaderElectionService} upon selecting this
	 * instance as the new leader. The method is called with the new leader session ID.
	 *
	 * @param leaderSessionID New leader session ID
	 * 当选举该节点为leader时，创建一个uuid,子类实现该leader的要做的逻辑,完成leader的切换工作
	 *
	 * 表示leader竞选成功的回调方法
	 */
	void grantLeadership(UUID leaderSessionID);

	/**
	 * Callback method which is called by the {@link LeaderElectionService} upon revoking the
	 * leadership of a former leader. This might happen in case that multiple contenders have
	 * been granted leadership.
	 * 表示由leader变为非leader的回调方法      说明该节点不是leader了
	 */
	void revokeLeadership();

	/**
	 * Returns the address of the {@link LeaderContender} under which other instances can connect
	 * to it.
	 *
	 * @return Address of this contender.
	 * 竞争者地址
	 */
	String getAddress();

	/**
	 * Callback method which is called by {@link LeaderElectionService} in case of an error in the
	 * service thread.
	 *
	 * @param exception Caught exception
	 * 当竞选过程出现任何问题的时候，回调
	 */
	void handleError(Exception exception);
}
