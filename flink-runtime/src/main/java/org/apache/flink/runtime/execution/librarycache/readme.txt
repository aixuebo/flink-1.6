
一、FlinkUserCodeClassLoaders 定义类加载器方式
1.PARENT_FIRST  = ParentFirstClassLoader
正常的类加载器,由root加载器先加载。避免加载器篡改flink或者scala等源码
2.CHILD_FIRST = ChildFirstClassLoader
a.private final String[] alwaysParentFirstPatterns;
需要去父类加载jar ，即他是flink需要的jar  只是全路径的开始部分即可,比如org.apache.flink.runtime,此时runtime下所有的包都由flink加载器加载
避免加载器篡改flink或者scala等源码
b.非alwaysParentFirstPatterns前缀的class,都是由子加载器加载。不需要向上root开始加载。

