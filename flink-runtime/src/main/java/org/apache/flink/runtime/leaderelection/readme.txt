一、目标
选举出leader的服务

二、LeaderElectionService 选举服务
void start(LeaderContender contender) throws Exception;//打开一个zookeeper客户端,参与选举
void stop() throws Exception;//该客户端停止参与选举
void confirmLeaderSessionID(UUID leaderSessionID); //确认该客户端是leader并且告诉该客户端的leader的id
boolean hasLeadership(@Nonnull UUID leaderSessionId);//true 表示该节点是leader节点 && leaderid与参数相同

三、LeaderContender 当选举服务显示该节点一定是leader的时候,则调用该类的实现类，起到通知作用。
	void grantLeadership(UUID leaderSessionID);//当选举该节点为leader时，创建一个uuid,子类实现该leader的要做的逻辑,完成leader的切换工作
	void revokeLeadership();//说明该节点不是leader了
	String getAddress();//竞争者地址


四、ZooKeeperLeaderElectionService 一种LeaderElectionService服务实现方式
1.利用zookeeper的选举能力,实现选举功能。
	private final LeaderLatch leaderLatch;//zookeeper自带的选举器 --- 可以知道该节点是否是leader节点
	private final String leaderPath;//选举服务的base path  存储当前leader的信息
2.当zookeeper选举完成,则会通知该节点是否选举成功/失败
isLeader() 选举成功,leaderContender.grantLeadership(issuedLeaderSessionID);通知
notLeader() 选举失败,leaderContender.revokeLeadership();通知

3.将leader的信息输出到leaderPath下,输出leader的地址+id



-----------
LeaderElectionService 服务提供的能力。
1.开启竞选start
2.当选择成功，则调isLeader()，产生一个issuedLeaderSessionID = UUID.randomUUID();
该uuid通知竞选者。
竟选择做初始化操作，比如开启master服务，开启成功后，会将uuid传给服务confirmLeaderSessionID()，说明leader确定可以对外服务了。
3.confirmLeaderSessionID(UUID leaderSessionID)
当收到竞选者提供的uuid后，更新confirmedLeaderSessionID = leaderSessionID;
writeLeaderInformation(confirmedLeaderSessionID);并且将uuid和leader对外暴露的master服务地址写到zookeeper上。
4.当uuid从leader变成非leader,调用notLeader()。
leaderContender.revokeLeadership();通知竞选者他不是leader了。

总结:该服务只是在zookeeper上做竞选，一旦成功后，会回调竞选者，竞选者做一些操作后，如果操作都成功，会再回调给服务，通知确定可以服务了，此时就会将master的leader信息写到zookeeper
上。这样外部任何服务就永远都都知道master是谁了。。master具备的能力是可以随时启动一个服务，恢复元数据的能力。
