一、Message包
相当于定义了requets、response的实体对象，在parameter上,感觉有点过度封装的感觉。
1.MessageParameter 表示存储key=value这类映射参数信息
a.MessagePathParameter 表示value是一个简单的值。比如int、double、string
b.MessageQueryParameter 表示value是一个List集合值。

2、MessageParameters
String resolveUrl(String genericUrl, MessageParameters parameters)
对给定url,将需要动态参数配置的信息，替换成具体的参数值。

3、请求
RestHandlerSpecification 描述请求往哪里发,以及什么方式发,即post/get 以及 url
RequestBody 定义是一个请求,具体请求内容是什么。
ResponseBody 定义一个回复,具体回复内容是什么。


4.核心请求类
MessageHeaders< RequestBody,ResponseBody,MessageParameters> extends RestHandlerSpecification
表示一个具体的请求,即有请求的url、请求的url动态参数、请求体RequestBody内容、请求回复内容ResponseBody。


二、基础类
1.public class RestClient
属于客户端,需要去请求服务端(RestServerEndpoint)
主要对外提供sendRequest方法 --- 属于工具类,提供了发送请求、返回respnse的能力
2.RestServerEndpoint
指代服务端暴露的服务 --- 接收客户端发送的请求,返回response给客户端
