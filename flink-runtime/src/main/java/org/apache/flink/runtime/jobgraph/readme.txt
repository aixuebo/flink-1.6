一、JobVertex 是一个定点，表示一组计算。
JobVertex：经过优化后符合条件的多个StreamNode可能会chain在一起生成一个JobVertex，
即一个JobVertex包含一个或多个operator，JobVertex的输入是JobEdge，输出是IntermediateDataSet。

输入：JobEdge或者IntermediateDataSet
输出：IntermediateDataSet

二、IntermediateDataSet
表示JobVertex的输出，即经过operator处理产生的数据集
是由JobVertex生产，由JobEdge消费。传到下一个JobVertex去消费。

三、JobEdge
代表了job graph中的一条数据传输通道。
输入是 IntermediateDataSet，
输出是JobVertex。
即数据通过JobEdge由IntermediateDataSet传递给目标JobVertex。
