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

package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.util.Preconditions;

import java.util.Collection;
import java.util.HashSet;

//注册一个taskManager,即报告一个taskManager是谁,以及她有多少个slot
public class TaskManagerRegistration {

	private final TaskExecutorConnection taskManagerConnection;//如何连接该task manager,与该task manager的网关交互

	private final HashSet<SlotID> slots;//该taskManager持有的slot集合

	private int numberFreeSlots;//该taskManager持有的slots集合,有多少个是空闲的

	/** Timestamp when the last time becoming idle. Otherwise Long.MAX_VALUE.
	 * 如果该值是Long.MAX_VALUE. 说明slot有被使用
	 **/
	private long idleSince;//当所有的slots都是空闲时,设置时间戳

	public TaskManagerRegistration(
		TaskExecutorConnection taskManagerConnection,
		Collection<SlotID> slots) {

		this.taskManagerConnection = Preconditions.checkNotNull(taskManagerConnection, "taskManagerConnection");
		Preconditions.checkNotNull(slots, "slots");

		this.slots = new HashSet<>(slots);

		this.numberFreeSlots = slots.size();

		idleSince = System.currentTimeMillis();//因为刚刚初始化一个taskManager,因此slot都是空闲的
	}

	public TaskExecutorConnection getTaskManagerConnection() {
		return taskManagerConnection;
	}

	//本地节点,针对taskManager的唯一ID
	public InstanceID getInstanceId() {
		return taskManagerConnection.getInstanceID();
	}

	//该task manager上注册了多少个slot可以用
	public int getNumberRegisteredSlots() {
		return slots.size();
	}

	//空闲可用的slot数量
	public int getNumberFreeSlots() {
		return numberFreeSlots;
	}

	//释放一个slot --- 增加slot的待使用池
	public void freeSlot() {
		Preconditions.checkState(
			numberFreeSlots < slots.size(),
			"The number of free slots cannot exceed the number of registered slots. This indicates a bug.");
		numberFreeSlots++;//释放一个slot

		//判断是否该task是空闲的
		if (numberFreeSlots == getNumberRegisteredSlots() && idleSince == Long.MAX_VALUE) {//说明slot都是空闲的,因此设置时间戳
			idleSince = System.currentTimeMillis();
		}
	}

	//使用一个slot -- 减少slot
	public void occupySlot() {
		Preconditions.checkState(
			numberFreeSlots > 0,
			"There are no more free slots. This indicates a bug.");
		numberFreeSlots--;

		idleSince = Long.MAX_VALUE;//说明slot有被使用
	}

	public Iterable<SlotID> getSlots() {
		return slots;
	}

	public long getIdleSince() {
		return idleSince;
	}

	//true表示该taskmanager的所有slot都空闲
	public boolean isIdle() {
		return idleSince != Long.MAX_VALUE;
	}

	//设置idleSince为long最大值，说明该task manager上有slot已经被使用了
	public void markUsed() {
		idleSince = Long.MAX_VALUE;
	}

	//参数slot是否在该task manager上
	public boolean containsSlot(SlotID slotId) {
		return slots.contains(slotId);
	}
}
