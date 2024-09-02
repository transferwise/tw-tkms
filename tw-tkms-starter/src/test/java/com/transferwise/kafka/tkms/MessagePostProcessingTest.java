package com.transferwise.kafka.tkms;

import com.transferwise.common.baseutils.transactionsmanagement.ITransactionsHelper;
import com.transferwise.kafka.tkms.api.ITransactionalKafkaMessageSender;
import com.transferwise.kafka.tkms.api.TkmsMessage;
import com.transferwise.kafka.tkms.test.BaseIntTest;
import com.transferwise.kafka.tkms.test.ITkmsSentMessagesCollector;
import com.transferwise.kafka.tkms.test.TestProperties;
import java.nio.charset.StandardCharsets;
import java.util.stream.StreamSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;

import static com.transferwise.kafka.tkms.test.TestKafkaProducerPostProcessor.TEST_MESSAGE;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MessagePostProcessingTest extends BaseIntTest {


    @Autowired
    protected ITkmsSentMessagesCollector tkmsSentMessagesCollector;
    @Autowired
    private TransactionalKafkaMessageSender transactionalKafkaMessageSender;
    @Autowired
    private TestProperties testProperties;
    @Autowired
    private ITransactionsHelper transactionsHelper;

    @BeforeEach
    void setupTest() {
      tkmsSentMessagesCollector.clear();
    }

    @AfterEach
    void cleanupTest() {
      tkmsSentMessagesCollector.clear();
    }

    @Test
    void messagesAreInstrumentedWithProducerPostProcessor() {
        byte[] someValue = TEST_MESSAGE;

        String topic = testProperties.getTestTopic();

        transactionsHelper.withTransaction().run(() ->
                transactionalKafkaMessageSender.sendMessages(new ITransactionalKafkaMessageSender.SendMessagesRequest()
                        .addTkmsMessage(new TkmsMessage().setTopic(topic).setKey("1").setValue(someValue))
                        .addTkmsMessage(new TkmsMessage().setTopic(topic).setKey("2").setValue(someValue))
                ));

        await().until(() -> tkmsSentMessagesCollector.getSentMessages(topic).size() == 2);
        var messages = tkmsSentMessagesCollector.getSentMessages(topic);

        assertEquals(2, messages.size());
        checkForHeader(messages.get(0), "wrapTest", "wrapped");
        checkForHeader(messages.get(1), "wrapTest", "wrapped");
    }

    private void checkForHeader(ITkmsSentMessagesCollector.SentMessage sentMessage, String key, String value) {
        assertTrue(
                StreamSupport.stream(sentMessage.getProducerRecord().headers().spliterator(), false)
                        .anyMatch(h -> h.key().equals(key) && value.equals(new String(h.value(), StandardCharsets.UTF_8)))
        );
    }
}
