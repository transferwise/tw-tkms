# Tests

TwTkms also provides helpers for conducting end-to-end integration tests.

For that, you can add a dependency.
```groovy
testImplementation 'com.transferwise.kafka:tw-tkms-test-starter'
```

And you will get the following beans.
```java
@Autowired
protected ITkmsSentMessagesCollector tkmsSentMessagesCollector;
@Autowired
protected ITkmsRegisteredMessagesCollector tkmsRegisteredMessagesCollector;
@Autowired
protected ITkmsTestDao tkmsTestDao
```

Consult those classes JavaDocs for more information about usages.

Usually you are using only `ITkmsSentMessagesCollector` though.

But `ITkmsRegisteredMessagesCollector` can be useful, if you have transactional tests which get rolled back at the end.

For example:
```java
public PayoutBatchStateChangedEvent waitForNinjasHasReceivedBatchStateChange(Long batchId) {
    return await().until(() -> {
        List<PayoutBatchStateChangedEvent> sentKafkaMessages = tkmsSentMessagesCollector.getSentJsonMessages(
                PAYOUT_BATCH_STATE_CHANGED, PayoutBatchStateChangedEvent.class);

        List<PayoutBatchStateChangedEvent> list = sentKafkaMessages.stream().filter((m) -> m.getBatchId().equals(batchId)).collect(Collectors.toList());
        return list.size() == 1 ? list.get(0) : null;
    }, Objects::nonNull);
}
```

Make sure, you are cleaning the memory regularly. For example after (or before) each test:
```java
tkmsRegisteredMessagesCollector.clear()
tkmsSentMessagesCollector.clear()
```

> If you don't the TwTkms will start yelling at you.