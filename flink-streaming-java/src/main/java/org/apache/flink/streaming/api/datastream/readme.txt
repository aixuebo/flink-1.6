一、DataStream --> 多个StreamTransformation组成
StreamTransformation 有时候包含Operation



1.union流
new DataStream<>(this.environment, new UnionTransformation<>(unionedTransforms))

2.SplitStream流
new SplitStream(ExecutionEnvironment,new SplitTransformation(OutputSelector))

