# nedis
An event-driven, asynchronous redis client based on [netty](http://netty.io/)

## How to use
Add this to your pom.xml. We deploy nedis to https://oss.sonatype.org.
```xml
<dependency>
  <groupId>com.wandoulabs.nedis</groupId>
  <artifactId>nedis-client</artifactId>
  <version>0.1.1</version>
</dependency>
```
To use it
```java
NedisClientPool nedisPool = NedisClientPoolBuilder.create().timeoutMs(5000)
        .remoteAddress("rediserver", 6379).build();
NedisClient nedis = NedisUtils.newPooledClient(nedisPool);
nedis.set(NedisUtils.toBytes("foo"), NedisUtils.toBytes("bar")).sync();
byte[] value = nedis.get(NedisUtils.toBytes("foo")).sync().getNow();
System.out.println(NedisUtils.bytesToString(value));
nedis.close().sync();
```

**Java7 is required to build or use nedis.**

## For codis users
Add this to your pom.xml.
```xml
<dependency>
  <groupId>com.wandoulabs.nedis</groupId>
  <artifactId>codis-client</artifactId>
  <version>0.1.1</version>
</dependency>
```
To use it
```java
RoundRobinNedisClientPool nedisPool = RoundRobinNedisClientPool.builder()
        .poolBuilder(NedisClientPoolBuilder.create().timeoutMs(5000)).curatorClient("zkserver:2181", 30000)
        .zkProxyDir("/zk/codis/db_xxx/proxy").build().sync().getNow();
NedisClient nedis = NedisUtils.newPooledClient(nedisPool);
nedis.set(NedisUtils.toBytes("foo"), NedisUtils.toBytes("bar")).sync();
byte[] value = nedis.get(NedisUtils.toBytes("foo")).sync().getNow();
System.out.println(NedisUtils.bytesToString(value));
nedis.close().sync();
```

## Performance
Nedis is **NOT** faster than jedis, especially if you use it in a synchronized way. In nedis, the requests on the same connection will be pipelined automatically if possible, but it is still **NOT** faster than jedis if you explicitly use a pipeline when using jedis.

So please use nedis with caution.

If you are an expert of netty, you could try to use `EpollEventLoopGroup` and add `-Dio.netty.allocator.type=pooled`(default is `unpooled` in netty 4.0) when starting to increase performance.
