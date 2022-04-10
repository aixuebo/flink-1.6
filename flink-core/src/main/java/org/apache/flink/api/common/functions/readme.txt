一、普通function
1.MapFunction,I-->0转换
2.FlatMapFunction, I --> List<O> 转换
3.FilterFunction, I --> I 转换, true表示数据依然被保留,false表示数据会被丢弃
4.MapPartitionFunction,List<I> --> List<O>转换
5.JoinFunction,(I1,I2) --> O的转换
6.FlatJoinFunction,(I1,I2) --> List<O>的转换
7.CrossFunction,(I1,I2) --> O的转换 笛卡尔join


二、富函数


三、聚合函数
1.ReduceFunction,(I,I) --> I的转换,即I类型与merge类型,转换成I类型
应用在KeyedStream下。
2.FoldFunction,(I,O) --> O ,
即带有初始值的ReduceFunction && 允许输入和输出是不同的元素。

3.CoGroupFunction,(List<I>,List<I>) --> List<O>
  主要用于join操作,两个集合如何输出一个集合

4.AggregateFunction<IN, ACC, OUT>
聚合函数,元素I两两聚合成中间结果Acc,ACC转换成O

5.CombineFunction,List<I> --> O 转换. 属于合并操作,将集合转换成一个合并的聚合值
map端的结果进行merge。
Flink在reduce之前大量使用了Combine操作。
 Combine可以理解为是在map端的reduce的操作，对单个map任务的输出结果数据进行合并的操作
 combine是对一个map的
 map函数操作所产生的键值对会作为combine函数的输入，
 经combine函数处理后再送到reduce函数进行处理，
   减少了写入磁盘的数据量，同时也减少了网络中键值对的传输量

6.GroupCombineFunction,List<I> --> List<O> 转换
7.GroupReduceFunction
扩展知识
分组的reduce，即 GroupReduce  GroupReduceFunction
-- 这与Reduce的区别在于用户定义的函数会立即获得整个组。在组的所有元素上使用Iterable调用该函数，
并且可以返回任意数量的结果元素
public interface GroupReduceFunction<T, O> extends Function, Serializable {
    void reduce(Iterable<T> var1, Collector<O> var2) throws Exception;
}

-- 使group-reduce函数可组合，它必须实现GroupCombineFunction接口
-- GroupCombine转换是可组合GroupReduceFunction中组合步骤的通用形式。
它在某种意义上被概括为允许将输入类型I组合到任意输出类型O.
-- 相反，GroupReduce中的组合步骤仅允许从输入类型I到输出类型I的组合。
这是因为reduce步骤中， GroupReduceFunction 期望输入类型为I.
public interface GroupCombineFunction<IN, OUT> extends Function, Serializable {
    void combine(Iterable<IN> var1, Collector<OUT> var2) throws Exception;
}


四、注意:
1.GroupCombineFunction与MapPartitionFunction区别
a.MapPartitionFunction 不会分组,是把节点上所有的数据统一参与计算。
b.GroupCombineFunction 会分组,他的迭代器确保都是同一个相同的key,该数据用在map端做数据聚合,减少shuffle的数据量

2.ReduceFunction与CombineFunction区别
CombineFunction 可以输入和输出不同类型、将集合转换成一个合并值。
ReduceFunction 输入和输出必须相同类型、不需要收集输入的集合,而是每一个元素都做一次merge操作。

3.CrossFunction与JoinFunction的区别
join此算子为内连接，即某方存在另一方不存在的key，舍弃该key对应数据。
Cross 算子对两个数据流的数据类型不做一致性的要求，所有数据会两两结合，生成新的字符串。
      与Join不同，Join需要双方按照约定的键进行等值连接，Cross任意两两都可组合。

4.ReduceFunction与FoldFunction区别
a.FoldFunction 带有输出值的ReduceFunction
b.FoldFunction 允许输入和输出是不同的类型,ReduceFunction要求是相同的类型
c.两者都是元素两两参与merge


5.GroupCombineFunction、GroupReduceFunction区别
a.GroupCombineFunction --- 相当于map端reduce
不会shuffle数据,即不会重新分区,只是在map端进行一次合并相同key的数据操作
b.GroupReduceFunction -- 相当于map-reduce中的reduce操作。
数据要重新分区到下游,有shuffle操作的reduce。 如果子类也实现了GroupCombineFunction,则相当于会在map端先进行一次reduce操作



