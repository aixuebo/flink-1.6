一、内存存储方式--堆内存、堆外内存
MemoryType -- HEAP 、OFF_HEAP

二、读取/输出 字节数组的接口
1.读取/输出 字节数组的接口
DataInputView extends DataInput
int read(byte[] b, int off, int len)
int read(byte[] b)
void skipBytesToRead(int numBytes) throws IOException  必须要跳过这些字节，如果字节数量不满足,则抛异常
void write(DataInputView source, int numBytes) throws IOException; 将参数的字节内容输出走。

2.读取/输出 字节数组的接口 -- 追加可随意切换位置读取/输出
interface SeekableDataInputView extends DataInputView
void setReadPosition(long position);  可随时切换字节数组位置的输入流

三、因为二定义的DataInputView extends DataInput是基于字节数组的，而java一般都是InputStream。
那么需要一个工具类，让java传入任何InputStream都能把解耦

class DataInputViewStreamWrapper extends DataInputStream implements DataInputView {

	public DataInputViewStreamWrapper(InputStream in) {
		super(in);
	}

即有一个包装类，他可以将InputStream转换成DataInputStream。


具体的子类:
public class ByteArrayDataInputView extends DataInputViewStreamWrapper {

	@Nonnull
	private final ByteArrayInputStreamWithPos inStreamWithPos;//数据源 --- 会构建一个字节数组,刚开始是空的，然后不断向该数据设置值,然后不断读取被消费掉

	public ByteArrayDataInputView() {
		super(new ByteArrayInputStreamWithPos());//创建一个空的数据源
		this.inStreamWithPos = (ByteArrayInputStreamWithPos) in;
	}

    //为数据源设置内容，然后不断消费内容
	public ByteArrayDataInputView(@Nonnull byte[] buffer) {
		this(buffer, 0, buffer.length);
	}

	public int getPosition() {
		return inStreamWithPos.getPosition();
	}

	public void setPosition(int pos) {
		inStreamWithPos.setPosition(pos);
	}

	//每次通过设置setData,然后慢慢读取数据
	public void setData(@Nonnull byte[] buffer, int offset, int length) {
		inStreamWithPos.setBuffer(buffer, offset, length);
	}
}


四、具体的字节数组的数据源 --- 可以不断被重复设置新的数据，然后被读消费
class ByteArrayInputStreamWithPos extends InputStream


	protected byte[] buffer;//缓存字节内容
	protected int position;//当前处理的位置
	protected int count;//buffer的可操作性的end结束位置
	protected int mark = 0;//可reset的position位置

    //设置数据源内容
	public ByteArrayInputStreamWithPos(byte[] buffer, int offset, int length) {
		setBuffer(buffer, offset, length);
	}

	//读取一个字节,返回字节内容
	@Override
	public int read() {
		return (position < count) ? 0xFF & (buffer[position++]) : -1;
	}

	//从缓存中读取len个字节内容,存储到参数b的off之后
	//返回读取了多少个字节,读取的内容在参数b中
	@Override
	public int read(byte[] b, int off, int len) {
		Preconditions.checkNotNull(b);

		if (off < 0 || len < 0 || len > b.length - off) {
			throw new IndexOutOfBoundsException();
		}

		if (position >= count) { //没有数据可以被读取了
			return -1; // signal EOF
		}

		int available = count - position; //可以读取的内容

		if (len > available) { //只能读取多少个字节
			len = available;
		}

		if (len <= 0) {
			return 0;
		}

		System.arraycopy(buffer, position, b, off, len);//从buffer中读取数据到b中
		position += len;
		return len;//真正读取的字节长度
	}

	@Override
	public long skip(long toSkip) {
		long remain = count - position;

		if (toSkip < remain) {
			remain = toSkip < 0 ? 0 : toSkip;
		}

		position += remain;
		return remain;
	}


	@Override
	public void reset() {
		position = mark;
	}

	@Override
	public int available() {
		return count - position;
	}


	public void setBuffer(byte[] buffer, int offset, int length) {
		this.count = Math.min(buffer.length, offset + length);
		setPosition(offset);
		this.buffer = buffer;
		this.mark = offset;
	}
}

同理 ByteArrayOutputStreamWithPos 用于存储输出的字节数组。
ByteArrayDataOutputView、DataOutputViewStreamWrapper

五、字节数组与基础类型的转换工具
public class DataInputDeserializer implements DataInputView, java.io.Serializable 如何读取二进制数据源，将字节数组转换成 boolean、int、string等形式
public class DataOutputSerializer implements DataOutputView  支持基础类型的数据 转换成字节数组 ，存储到buffer中

六、class HeapMemorySegment extends MemorySegment 堆内内存
1.使用byte[]数组存储数据。
2.free() 释放,则将byte[]设置为null,让垃圾回收站回收。
3.产生一个子集--即从offset到offset+length截取数据
ByteBuffer wrap(int offset, int length)
return ByteBuffer.wrap(this.memory, offset, length);
4.返回字节数组内容 byte[] getArray()
5.get与put,即向内存中添加/读取数据
byte get(int index)  读取index位置的一个字节
get(int index, byte[] dst, int offset, int length) 从index位置开始读取数据,读取length个字节,存储到dst中。
get(int index, byte[] dst) = get(index, dst, 0, dst.length);
boolean getBoolean(int index)


void put(int index, byte b) memory[index] = b; 向byte中存储一个字节
put(int index, byte[] src, int offset, int length) 向byte中存储length个字节,从index位置开始存储，存储的内容来自于src
put(int index, byte[] src) = put(index, src, 0, src.length);将src的内容全部都写出到bytes中
putBoolean(int index, boolean value)

以及读取string、int、char等数据信息

6.高级读写功能

	//读取本地缓存数据,输出到out中。读取本地的数据从offset开始,读取length个
	@Override
	public final void get(DataOutput out, int offset, int length) throws IOException {
		out.write(this.memory, offset, length);
	}

	//读取数据,存储到target中,读取的数据范围是从offset开始读取,读取numBytes个数据
	@Override
	public final void get(int offset, ByteBuffer target, int numBytes) {
		// ByteBuffer performs the boundary checks
		target.put(this.memory, offset, numBytes);
	}

	//读取in的数据,插入到本都内存中,从本都的offset位置开始插入,插入length个数据
	@Override
	public final void put(DataInput in, int offset, int length) throws IOException {
		in.readFully(this.memory, offset, length);
	}

	//读取source数据,插入到本地memory中,从本地的offset开始插入,插入numBytes个数据
	@Override
	public final void put(int offset, ByteBuffer source, int numBytes) {
		// ByteBuffer performs the boundary checks
		source.get(this.memory, offset, numBytes);
	}

七、堆外内存 HybridMemorySegment extends MemorySegment

注意:堆内内存、堆外内存，都是交互字节数组，将字节数组转换成string、int等基础类型，或者基础类型转换成字节数组。
