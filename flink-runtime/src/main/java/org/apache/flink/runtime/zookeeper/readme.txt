把zookeeper当成一个数据库，存储对象的字节数组

主要目标是使用zookeeper持久化存储序列化后的对象。
可能序列化后对象很大,因此给出一套方案,存储到hdfs上,将存储后的HDFS的path，存储到zookeeper，这样完成持久化操作。


一、 ZooKeeperStateHandleStore<T extends Serializable>
 * 基于zookeeper存储数据。
 * 由于T序列化后可能会很大，都存储在zookeeper上不太合理。因此设计原理如下:
 * 1.将T序列化到hdfs等存储上。
 * 2.返回可以读取hdfs上信息，反序列化T对象的对象 RetrievableStateHandle，并且该对象支持序列化.（此时序列化就信息占用字节数组很小）
 * 3.zookeeper的path上存储序列化的对象RetrievableStateHandle
 */

将T对象，存储到path下:
1.T转换成字节数组，存储到hdfs上，返回RetrievableStreamStateHandle(Path filePath, long stateSize)。
2.将RetrievableStreamStateHandle(Path filePath, long stateSize)对象序列化成字节数组。存储到path下。

这样就可以通过path，返回RetrievableStreamStateHandle(Path filePath, long stateSize)，然后读取文件返回T对象。


二、FileSystemStateStorageHelper<T extends Serializable> implements RetrievableStateStorageHelper<T>
用于存储对象到hdfs上。 构造的时候需要传递hdfs的根目录 + 文件名字前缀

RetrievableStateHandle<T> store(T state) ：在hdfs上随机生成一个文件。然后输出字节数组到文件中。
返回new RetrievableStreamStateHandle<T>(filePath, outStream.getPos())  描述好对象存储到哪个文件中，以及文件中字节数组的大小。


三、RetrievableStreamStateHandle(Path filePath, long stateSize) 用于存储hdfs上的对象。
把对象转换成字节数组，存储到文件中。
对于内存就是返回一个对象RetrievableStreamStateHandle，他持有文件的路径。




