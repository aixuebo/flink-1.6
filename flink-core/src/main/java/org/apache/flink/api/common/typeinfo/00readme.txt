一、typeInformation<T> implements Serializable 基础类
对class的一个封装,可以知道class是谁，如何序列化该class、该对象能否支持排序等功能。

1.boolean isBasicType(); 是否基础类型
2.boolean isTupleType() 是否tuple类型
3.int getArity();有多少个元素 -- 不需要解析嵌套字段
4.int getTotalFields(); 逻辑上包含多少个字段，需要解析嵌套字段
5.Class<T> getTypeClass(); 到底是哪个class,因为涉及到泛型类型擦除,所以需要知道具体的class是谁
6.boolean isKeyType();true表示 类型可以比较，也可以hash
7. boolean isSortKeyType() true表示类型可以排序
8.TypeSerializer<T> createSerializer(ExecutionConfig config) 为给定对象T,创建序列化对象

二、BasicTypeInfo<T> extends TypeInformation<T> implements AtomicType<T> (如何排序Comparator)
java基础类型、char、string、Date、void、BigInteger、BigDecimal
private final Class<T> clazz;//真实类型 比如 Integer
private final TypeSerializer<T> serializer;//如何序列化
private final Class<?>[] possibleCastTargetTypes;//可以强转到什么类型,比如byte 强转成 int、short、long等
private final Class<? extends TypeComparator<T>> comparatorClass;//如何参与比较

三、数组
PrimitiveArrayTypeInfo<T> extends TypeInformation<T> implements AtomicType<T>  原始java基础类型 -- 组成的数组
BasicArrayTypeInfo<T, C> extends TypeInformation<T>  java基础类型的包装盒子对象--组成的数组


四、

WritableTypeInfo任意Hadoop的Writable接口的实现类。
TupleTypeInfo任意的Flink tuple类型(支持Tuple1 to Tuple25)。 Flink tuples是固定长度固定类型的Java Tuple实现。
CaseClassTypeInfo任意的 Scala CaseClass(包括 Scala tuples)。
PojoTypeInfo任意的POJO (Java or Scala)，例如Java对象的所有成员变量，要么是public修饰符定义，要么有getter/setter方法。
Row: 包含任意多个字段的元组并且支持null成员

2.GenericTypeInfo任意无法匹配之前几种类型的类。
泛型: Flink自身不会序列化泛型，而是借助Kryo进行序列化。

五、序列化 Serializer
前6种类型数据集几乎覆盖了绝大部分的Flink程序，针对前6种类型数据集，Flink皆可以自动生成对应的TypeSerializer定制序列化工具，非常有效率地对数据集进行序列化和反序列化。
对于第GenericTypeInfo种类型，Flink使用Kryo进行序列化和反序列化


六、排序 Comparator
Flink中的比较器不仅仅是定义大小顺序，更是处理keys的基本辅助工具

对于可被用作Key的类型，Flink还同时自动生成TypeComparator，用来辅助直接对序列化后的二进制数据直接进行compare、hash等操作。
对于Tuple、CaseClass、Pojo等组合类型，Flink自动生成的TypeSerializer、TypeComparator同样是组合的，并把其成员的序列化/反序列化代理给其成员对应的TypeSerializer、TypeComparator


七、应用
1.TypeInformation<String> info = TypeInformation.of(String.class);  ###返回基础string类的包装类
2.TypeInformation<Tuple2<String, Double>> info = TypeInformation.of(new TypeHint<Tuple2<String, Double>>(){});
在内部，这个操作创建了一个TypeHint的匿名子类，用于捕获泛型信息，这个子类会一直保存到运行时。






