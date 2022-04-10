一、基于数组实现的优先队列
内部是一个HeapPriorityQueueElement[]。保证数组元素是有顺序的。(添加/删除的时候,通过移动数组,确保依然有序)
通过元素的序号,代表在数组的顺序位置。

提供能力:
1.优先队列,天然的可以从top位置获取数据，并且依次从小到大的迭代元素。
2.通过HeapPriorityQueueElement的序号,可以快速从数组中还原某一个元素。

HeapPriorityQueueElement 添加的元素,追加一个索引位置信息。
AbstractHeapPriorityQueue 基于数组实现的优先队列。元素是HeapPriorityQueueElement
HeapPriorityQueue 具体的实现类,设置一个比较器的AbstractHeapPriorityQueue

注意:
该优先队列不控制容量,可以无限扩容,即一直add元素,都没有问题(只要内存充足),他只保证了排序,所以元素添加多了,会很卡,排序很耗时


二、HeapPriorityQueueSet 可以存放不重复的元素的优先队列
由于堆内存HeapPriorityQueue本身只是一个数组,确保了每一个元素添加后是有顺序的,但不能保证不重复。
因此扩展,先将数据存放到内部的map中,如果添加成功,说明数据不存在,因此可以放到优先队列里

注意:这里他会根据每一个key生成一个map,确保每一个key的数据不会重复,但多个key之间会有重复的，因此像是应用在merge操作。

三、stage
1.StateTable  <Key+命名空间> --> value 映射关系。
存储在内存中,因此里面都存储的是对象，当需要传输时,可以对其序列化成字节数组,传输后再反序列化
2.AbstractHeapState<K, N, SV>
满足基础 -- 如何序列化/反序列化key、命名空间、vakue值。  如果通过key+命名空间,获取value值对象

其中命名空间是提前set好的,key对象是可以通过上下文获取到的。因此只要保证StateTable是在内存可以拿到就ok。


四、StateTableByKeyGroupReaders  如何读取StateTable
