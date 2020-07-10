# Usage

You can autowire the following bean and consult its javadoc for how to use it:
 
```java
@Autowired
private ITransactionalKafkaMessageSender transactionalKafkaMessageSender;
```

You may also want to use the following to create Json messages without much effort:
```java
@Autowired
private ITkmsMessageFactory tkmsMessageFactory;
```

Most common use case will be sending a single message:
```java
transactionalKafkaMessageSender.sendMessage(tkmsMessageFactory.createJsonMessage(value).setTopic(topic).setKey(key));
```