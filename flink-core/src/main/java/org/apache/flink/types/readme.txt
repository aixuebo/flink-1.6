一、value extends IOReadableWritable, Serializable
对值的一种顶层抽象，支持流读写、支持序列化
1.必须能序列化
2.必须可以读写DataOutputView、DataInputView

二、ResettableValue<T extends Value> extends Value {
void setValue(T value);

三、Key<T> extends Value, Comparable<T>
可以参与比较的值

四、NormalizableKey<T> extends Comparable<T>, Key<T>
支持二进制每一个字节去比较的值

五、CopyableValue<T> extends Value
支持可复制的值

六、java基础对象的封装。
IntValue implements NormalizableKey<IntValue>, ResettableValue<IntValue>, CopyableValue<IntValue>
LongValue implements NormalizableKey<LongValue>, ResettableValue<LongValue>, CopyableValue<LongValue>
ShortValue implements NormalizableKey<ShortValue>, ResettableValue<ShortValue>, CopyableValue<ShortValue>
DoubleValue implements Comparable<DoubleValue>, ResettableValue<DoubleValue>, CopyableValue<DoubleValue>, Key<DoubleValue>
FloatValue implements Comparable<FloatValue>, ResettableValue<FloatValue>, CopyableValue<FloatValue>, Key<FloatValue>
BooleanValue implements NormalizableKey<BooleanValue>, ResettableValue<BooleanValue>, CopyableValue<BooleanValue>
ByteValue implements NormalizableKey<ByteValue>, ResettableValue<ByteValue>, CopyableValue<ByteValue>
CharValue implements NormalizableKey<CharValue>, ResettableValue<CharValue>, CopyableValue<CharValue>
StringValue implements NormalizableKey<StringValue>, CharSequence, ResettableValue<StringValue>,CopyableValue<StringValue>, Appendable

高级封装
ListValue<V extends Value> implements Value, List<V> 持有list 以及 value的class
MapValue<K extends Value, V extends Value> implements Value, Map<K, V> 持有key、value的class、以及 map

七、NullValue implements NormalizableKey<NullValue>, CopyableValue<NullValue>

八、Row implements Serializable
存储Object[] fields 表示一行具体的值集合。

九、Record implements Value, CopyableValue<Record>
相当于java中的vo类,我们知道定义vo的时候，是由若干个属性定义的，而若干个属性都是由基础类型+list+map等组成的,而这些属性类型我们都定义成value了。
因此我们可以使用Record代替vo。

假设Record中存储10个元素，其中8个元素是有值的,2个元素是null。则以下变量分别为:
1.private int numFields = 10.
2.private int[] offsets; ## length = 10,记录了每一个属性的开始偏移量。注意:如果元素没有设置值,则为null,该位置填充的是NULL_INDICATOR_OFFSET。如果该位置被修改了,还尚未序列化起来,则填充MODIFIED_INDICATOR_OFFSET
3.private int[] lengths;## length = 10,记录了每一个属性的字节长度。
4.private byte[] binaryData;存储序列化成功的字节数组,注意,按照顺序存储8个有值元素的数据。
5.private int binaryLen; 记录binaryData的偏移量.即binaryData到哪个offset前都是有效的。

因为元素会被修改或者新增加。因此不会每次都同步修改binaryData，因为成本较大，主要因为binaryData是按照非空的数据按顺序填充的，所以插入或修改的属性的时候，会涉及到字节数组的移动。因此会统一做定期同步。
因此会临时存储修改或新增的数据
6.private Value[] writeFields;## length = 10,记录了每一个属性的字节长度。记录了第几个元素是被修改的。
7.private Value[] readFields;;## length = 10,记录了已经序列化成value的值,因此如果没有修改,则不需要每次都反序列化。提高性能。
8.private int firstModifiedPos;记录最先被修改的元素位置。

使用流程:
定期同步时,因为firstModifiedPos之前的元素都没有被修改过，因此不需要序列化，直接复制字节数组即可。将非null的元素，复制到新的缓冲区。
firstModifiedPos之后的数据，从writeFields或者原始的字节数组中提取出来，反序列化到字节数组中。

使用方式:
<T extends Value> T getField(final int fieldNum, final Class<T> type)  直接返回第几个元素对应的value.相当于vo的get方法。
setField(int fieldNum, Value value) 相当于vo的set方法
boolean isNull(int fieldNum) 该属性是否为null,即未设置值
setNull(int field) 设置该属性为null


十、FieldParser 描述了从字节数组中如何解析成java基础属性，并且如果解析失败,会提示失败原因
参见 IntParser 具体的分析
