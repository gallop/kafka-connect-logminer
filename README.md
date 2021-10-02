# 项目代码说明

本项目代码使用kafka connect api 实现了 source from LogMiner and sink to oracle.  
1、logminer 的SourceTask，用SourceTask 获取logminer 解析oracle的redo 日志；  
2、JdbcSinkTask 将redo sql 执行到目标oracle 数据库；



## 1、logminer redo日志 to kafka

这节主要介绍 如何将logminer 解析的oracle redo日志 输入到kafka中，大概的思路是：构建一个继承SourceTask 的类，然后执行logminer 解析redo日志的相关sql语句，将返回的结果：redo 日志信息，封装成SourceRecord 对象，SourceTask类的 poll() 方法会被WorkerSourceTask类中的方法定期调用，获取SourceRecord 列表，进行kafka 入队列操作，并管理相关的offset。  
> 注意：要运行此节的代码的前提，  
> 是oracle 数据库要能运行logminer 的相关sql语句，详细配置看：
> [数据同步，oracle 数据库logminer配置]()

### 1.1 要点介绍

主要逻辑要点：
1、LogMinerSourceConnector.java 是source 程序的入口；  
2、LogMinerSourceTask.java 是负责将源数据提交到kafka主题的逻辑接口，主要方法是poll() 方法，WorkerSourceTask类会轮询调用此方法，进行提交kafka操作，代码如下：  

```
@Override
	public List<SourceRecord> poll() throws InterruptedException {
		try {
			List<LogMinerEvent> events = session.poll(this.state);
			if (events == null || events.size()==0)
				return null;
			LOGGER.info("---------"+Thread.currentThread().getName()+"--Polling for new events-size="+events.size());
			// TODO: either consider implementing topic prefix of some sort, or remove
			// config option in favour of one topic partitioned by table
      // 这里我们要做的只是从源数据获取数据，并封装成SourceRecord 的list，kafka connect 会帮我们
      //提交到kafka 并自动做offset 管理。
			return events.stream()
					.map(e -> {
						updateCurrentState(e.getPartition(),e.getOffsetObject());
						return new SourceRecord(e.getPartition(), e.getOffset(),
							config.getString(LogMinerSourceTaskConfig.TOPIC_CONFIG), e.getSchema(), e.getStruct());
					}).collect(Collectors.toList());
		} catch (Exception e) {
			throw new ConnectException("Error during LogMinerSourceTask poll", e);
		}
	}

```

> 注意：在Standalone 单机模式下，offset 都是存在配置文件offset.storage.file.filename字段指定的文件中的，  
> Standalone 配置文件路径：在kafka安装目录的config/connect-standalone.properties。  
> 另外，本文的所有测试都是基于Standalone模式下的，切记！切记！

3、LogMinerSession.java 主要是负责oracle的数据库连接，并执行logminer的查询sql语句，并返回查询结果，上面的代码就是从此类获取logminer的redo 日志信息。  
4、resources 目录下的sql语句，这里主要是logminer的相关的sql 脚本，所有的logminer的sql脚本执行都从这里获取。
5、logminer.source.properties 配置文件，本文件主要是指定了程序入口的类路径、数据库连接、提交到kafka的topic 名字、已经需要logminer 解析的源数据库表，详细如下：
```
# Common connector config
name=kafka-connect-logminer
# 程序的入口类指定
connector.class=com.gallop.connect.logminer.LogMinerSourceConnector
tasks.max=1

# Database，数据库连接
connection.url=jdbc:oracle:thin:@192.168.0.117:1521:orcl
connection.user=cdcuser
connection.password=cdcuser
db.fetch.size=1
#需要解析的表，多个用逗号隔开
table.whitelist=test_user
table.blacklist=

# 提交kafka的topic名称
topic=testdb.events
topic.prefix=
parse.dml.data=true

# LogMiner behaviour
#
# The system change number to start at.  Earliest available: min; latest available: current;
# next available: next; specific SCN: long
scn=
# The LogMiner dialect to use; "single" or "multitenant"
dialect=single

```

### 1.2 执行本节代码步骤

代码是基于kafka connect的，所以代码的运行是用kafka bin目录下的connect-standalone.sh 脚本来启动的。主要的步骤如下：  
1、logminer.source.properties 配置文件拷贝到kafka安装路径的config目录下；   
2、将项目打成jar包，拷贝到kafka安装目录的libs目录下；  
3、将依赖jar包，拷贝到kafka安装目录的libs目录下；    
这里涉及到的依赖jar包：
```
fastjson-1.2.75.jar
jsqlparser-2.1.jar
ojdbc8-12.2.0.1.jar

```
4、执行脚本，启动程序
```
./connect-standalone.sh ../config/connect-standalone.properties ../config/logminer.source.properties

```
> 注意：./connect-standalone.sh 后面可以跟着多个配置文件，用空格隔开！



## 2、kafka redo日志 sink to oracle
这节主要介绍 消费kafka中的redo 日志，并通过connect-jdbc 将redo-sql 在目标oracle执行，以达到实时同步oracle 数据表的目的。大概的思路是：创建一个继承SinkTask的类，并在类的方法  
public void put(Collection<SinkRecord> records)   
持续处理来自kafka队列中的records 数据。  

### 2.1 要点介绍

1、程序的入口类是：JdbcSinkConnector.java   
2、数据处理的主要逻辑入口： JdbcSinkTask.java  
在这里主要是在put(Collection<SinkRecord> records)方法，消费处理kafka队列的消息（在kafka connect中的WorkerSourceTask类的iteration()方法，将持续的消费kafka消息，并调用put方法，将消息传递给JdbcSinkTask类做处理）。  
3、JdbcDbWriter 类，主要负责目标数据库的连接和执行从kafka 消费出来的redo sql语句，这里是在kafka-connect-jdbc代码的基础上进行的更改。  
4、JdbcSinkTask类中的ErrantRecordReporter主要是负责记录消费异常的信息，如果要起作用必须满足一下几点：
- kafka 版本必须2.6.0 及以后的版本；
- 需要预先创建要存储错误消息的死信队列topic
- 要在配置文件做相应的配置（这里的配置文件是config目录下的logminer-sink-oracle.properties），内容如下：

```
# log error context along with application logs, but do not include configs and messages
#ErrantRecordReporter 要生效的配置：
errors.log.enable=true
errors.log.include.messages=false

# produce error context into the Kafka topic，记录错误消息的死信队列，需要预先创建好topic
errors.deadletterqueue.topic.name=my-connector-errors

# Tolerate all errors.
errors.tolerance=all

```

5、如果执行消息处理异常，可以在put(Collection<SinkRecord> records)方法里，向kafka connect 抛RetriableException()异常，kafka connect会重新在调用一次put方法，以达到重试的目的（实际测试发现，确实是重新调用put方法了，但重试次数达到设定的次数后把消息发送到死信队列后，消息的offset却没法正常的提交，不知是kafka connect的bug还是没找到正确的使用方法？？，所以代码中放弃了消息消费异常后重试的次数设置，直接进死信队列了，在注释掉的代码可查看。）

### 2.2 执行本节代码步骤

代码仍然是用kafka bin目录下的connect-standalone.sh 脚本来启动的。主要的步骤如下：  
1、logminer-sink-oracle.properties 配置文件拷贝到kafka安装路径的config目录下；   
2、将项目打成jar包，拷贝到kafka安装目录的libs目录下；  
3、将依赖jar包，拷贝到kafka安装目录的libs目录下；    
4、执行脚本，启动程序
```
./connect-standalone.sh ../config/connect-standalone.properties ../config/logminer-sink-oracle.properties

```


**注意：**  

在正式运行的环境中，source 和 sink 的代码是需要同时在运行的，运行的脚本如下：
```
./connect-standalone.sh ../config/connect-standalone.properties ../config/logminer.source.properties ../config/logminer-sink-oracle.properties

```