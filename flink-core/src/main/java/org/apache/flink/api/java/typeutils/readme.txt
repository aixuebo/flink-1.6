一、如何将一个java对象转换成可序列化的PojoTypeInfo
参见 PojoTypeInfoTest
	protected PojoTypeInfo<?>[] getTestData() {
		return new PojoTypeInfo<?>[] {
			(PojoTypeInfo<?>) TypeExtractor.getForClass(TestPojo.class),
			(PojoTypeInfo<?>) TypeExtractor.getForClass(AlternatePojo.class),
			(PojoTypeInfo<?>) TypeExtractor.getForClass(PrimitivePojo.class),
			(PojoTypeInfo<?>) TypeExtractor.getForClass(UnderscorePojo.class)
		};
	}
