package com.transferwise.kafka.tkms;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.transferwise.common.baseutils.UuidUtils;
import com.transferwise.common.baseutils.clock.TestClock;
import com.transferwise.common.baseutils.transactionsmanagement.ITransactionsHelper;
import com.transferwise.kafka.tkms.api.TkmsMessage;
import com.transferwise.kafka.tkms.api.TkmsShardPartition;
import com.transferwise.kafka.tkms.config.ITkmsDaoProvider;
import com.transferwise.kafka.tkms.config.TkmsProperties;
import com.transferwise.kafka.tkms.metrics.ITkmsMetricsTemplate;
import com.transferwise.kafka.tkms.test.BaseIntTest;
import com.transferwise.kafka.tkms.test.TestLogAppender;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles("earliest-message")
@Slf4j
class EarliestMessageTrackingIntTest extends BaseIntTest {

  @Autowired
  private TransactionalKafkaMessageSender tkms;
  @Autowired
  private ITkmsDaoProvider tkmsDaoProvider;
  @Autowired
  private TkmsProperties properties;
  @Autowired
  private ITkmsMetricsTemplate metricsTemplate;
  @Autowired
  private ITransactionsHelper transactionsHelper;

  @Value("${tw-tkms-test.test-topic}")
  private String testTopic;

  private int sentMessagesCount = 0;

  private boolean pollingAllRecordsHappened;

  @BeforeEach
  public void setupAll() {
    Debug.setEarliestMessagesTrackerDebugEnabled(true);
    TestLogAppender.setEventFilter(event -> {
      if (event.getMessage().startsWith("Polling all messages for")) {
        pollingAllRecordsHappened = true;
      }
      return true;
    });
  }

  @AfterEach
  public void cleanupAll() {
    Debug.setEarliestMessagesTrackerDebugEnabled(false);
  }

  @Test
  void testIfEarliestMessageTrackerBehavesAsExpected() {
    var clock = new TestClock(Instant.now());
    TkmsClockHolder.setClock(clock);

    var earliestMessageIdGauge = await()
        .until(() -> meterRegistry.find("tw_tkms_dao_earliest_message_id").tags("shard", "0", "partition", "0").gauge(), Objects::nonNull);
    assertThat(earliestMessageIdGauge.value()).isEqualTo(-1);

    sendMessageAndWaitForArrival();
    clock.tick(Duration.ofSeconds(5));
    sendMessageAndWaitForArrival();

    assertThat(earliestMessageIdGauge.value()).as("First period has not passed, so earliest message id is not yet usable.").isEqualTo(-1);

    clock.tick(Duration.ofSeconds(6));
    sendMessageAndWaitForArrival();
    assertThat(earliestMessageIdGauge.value()).as("First 10s period has passed, we should have a id now").isNotNegative();

    double previousValue = earliestMessageIdGauge.value();
    clock.tick(Duration.ofSeconds(6));
    sendMessageAndWaitForArrival();
    assertThat(earliestMessageIdGauge.value()).isGreaterThanOrEqualTo(previousValue);

    clock.tick(Duration.ofSeconds(6));
    sendMessageAndWaitForArrival();
    assertThat(earliestMessageIdGauge.value()).isGreaterThan(previousValue);

    var committedValue =
        jdbcTemplate.queryForObject("select message_id from earliestmessage.tw_tkms_earliest_visible_messages where shard=? and part=?", Long.class,
            0, 0);
    assertThat(committedValue).isGreaterThanOrEqualTo((long) previousValue);

    var tkmsDao = tkmsDaoProvider.getTkmsDao(0);
    var earliestMessageTracker = new EarliestMessageTracker(tkmsDao, TkmsShardPartition.of(0, 0), properties, metricsTemplate);
    earliestMessageTracker.init();

    assertThat(earliestMessageTracker.getEarliestMessageId()).isEqualTo(committedValue);

    assertThat(pollingAllRecordsHappened);
  }

  protected void sendMessageAndWaitForArrival() {
    transactionsHelper.withTransaction().run(() -> {
          var result = tkms.sendMessage(
              new TkmsMessage()
                  .setUuid(UuidUtils.generatePrefixCombUuid())
                  .setTopic(testTopic)
                  .setValue("Hello Kristo!".getBytes(StandardCharsets.UTF_8))
          );
          log.info("Registered a message with storage id " + result.getStorageId());
        }
    );

    sentMessagesCount++;

    await().until(() -> tkmsSentMessagesCollector.getSentMessages(testTopic).size() == sentMessagesCount);
  }
}
