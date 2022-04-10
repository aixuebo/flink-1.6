一、PendingSlotRequest
每一个job manager发送的资源申请，都不会立刻给返回资源，因为可能当时也没有资源，需要去yarn上申请，因此都会放到一个队列里。

1.队列会有超时时间，如果超过一定时间后还没分配资源给jobmanager，则job manager会重新发送请求。
2.持有属性
final SlotRequest slotRequest;//缓存持有请求的资源
final long creationTimestamp;//进入队列的时间
CompletableFuture<Acknowledge> requestFuture;//返回请求的资源

资源归属: 哪个job、请求的id、申请的资源大小、job manager的leader节点对外开放的地址
JobID getJobId()
AllocationID getAllocationId()
ResourceProfile getResourceProfile()
String getTargetAddress()


二、TaskManagerRegistration
当task manager申请到资源后,会请求resource manager,告诉资源池有多少资源可以用。

1.持有属性
TaskExecutorConnection taskManagerConnection;//如何连接该task manager,与该task manager的网关交互
HashSet<SlotID> slots;//该taskManager持有的slot集合
int numberFreeSlots;//该taskManager持有的slots集合,有多少个是空闲的

2.动作
使用一个slot、释放一个slot。即同步slot的空闲数量

三、

