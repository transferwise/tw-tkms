package com.transferwise.kafka.tkms;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.transferwise.common.baseutils.transactionsmanagement.ITransactionsHelper;
import com.transferwise.kafka.tkms.api.ITransactionalKafkaMessageSender;
import com.transferwise.kafka.tkms.api.TkmsMessage;
import com.transferwise.kafka.tkms.config.ITkmsKafkaProducerProvider;
import com.transferwise.kafka.tkms.test.BaseIntTest;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;

public class MultiServerTopicValidationIntTest extends BaseIntTest {

  @Autowired
  private ITransactionalKafkaMessageSender transactionalKafkaMessageSender;
  @Autowired
  private ITransactionsHelper transactionsHelper;
  @Autowired
  private ITkmsKafkaProducerProvider tkmsKafkaProducerProvider;

  @AfterEach
  public void cleanup() {
    super.cleanup();

    tkmsProperties.getTopicValidation().setUseAdminClient(false);
    tkmsProperties.getTopicValidation().setTryToAutoCreateTopics(true);
  }

  private static Stream<Arguments> unknownTopicsMatrix() {
    return Stream.of(
        Arguments.of(true),
        Arguments.of(false)
    );
  }

  @ParameterizedTest
  @MethodSource("unknownTopicsMatrix")
  @SneakyThrows
  void sendingToUnknownTopicWillBePreventedWhenTopicAutoCreationIsDisabled(boolean useAdminClient) {
    tkmsProperties.getTopicValidation().setUseAdminClient(useAdminClient);

    final var notExistingTopic = "NotExistingTopic";
    final var existingTopicInKafka2 = "TestTopicInAnotherServer";

    assertThatThrownBy(() -> sendMessage(notExistingTopic, null)).hasMessageContaining(notExistingTopic);

    sendMessage("TestTopic", null);
    sendMessage("TestTopic", tkmsProperties.getDefaultShard());

    assertThatThrownBy(() -> sendMessage(existingTopicInKafka2, null)).hasMessageContaining(existingTopicInKafka2);

    sendMessage(existingTopicInKafka2, 2);
  }

  protected void sendMessage(String topic, Integer shard) {
    try {
      transactionsHelper.withTransaction().run(() -> {
        var message = new TkmsMessage().setTopic(topic).setValue("Stuff".getBytes(StandardCharsets.UTF_8));
        if (shard != null) {
          message.setShard(shard);
        }
        transactionalKafkaMessageSender.sendMessage(message);
      });
    } catch (Exception e) {
      throw new RuntimeException("Sending message to topic '" + topic + "' and shard " + shard + " failed.");
    } finally {
      // Stop spam of metadata errors.
      tkmsKafkaProducerProvider.closeKafkaProducersForTopicValidation();
    }
  }
}
