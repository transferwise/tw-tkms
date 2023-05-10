package com.transferwise.kafka.tkms;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.transferwise.common.baseutils.clock.TestClock;
import com.transferwise.common.baseutils.transactionsmanagement.ITransactionsHelper;
import com.transferwise.kafka.tkms.api.TkmsMessage;
import com.transferwise.kafka.tkms.api.TkmsShardPartition;
import com.transferwise.kafka.tkms.config.ITkmsDaoProvider;
import com.transferwise.kafka.tkms.config.TkmsProperties;
import com.transferwise.kafka.tkms.metrics.ITkmsMetricsTemplate;
import com.transferwise.kafka.tkms.test.BaseIntTest;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles("earliest-message")
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

  @Test
  void testIfEarliestMessageTrackerBehavesAsExpected() {
    var clock = new TestClock();
    TkmsClockHolder.setClock(clock);

    var earliestMessageIdGauge = await()
        .until(() -> meterRegistry.find("tw_tkms_dao_earliest_message_id").tags("shard", "0", "partition", "0").gauge(), Objects::nonNull);
    assertThat(earliestMessageIdGauge.value()).isEqualTo(-1);

    clock.tick(Duration.ofSeconds(5));
    sendMessageAndWaitForArrival(1);

    assertThat(earliestMessageIdGauge.value()).as("First period has not passed, so earliest message id is not yet usable.").isEqualTo(-1);

    clock.tick(Duration.ofSeconds(6));
    sendMessageAndWaitForArrival(2);
    assertThat(earliestMessageIdGauge.value()).as("First 10s period has passed, we should have a id now").isNotNegative();

    double previousValue = earliestMessageIdGauge.value();
    clock.tick(Duration.ofSeconds(6));
    sendMessageAndWaitForArrival(3);
    assertThat(earliestMessageIdGauge.value()).isGreaterThanOrEqualTo(previousValue);

    clock.tick(Duration.ofSeconds(6));
    sendMessageAndWaitForArrival(4);
    assertThat(earliestMessageIdGauge.value()).isGreaterThan(previousValue);

    var committedValue =
        jdbcTemplate.queryForObject("select message_id from earliestmessage.tw_tkms_earliest_visible_messages where shard=? and part=?", Long.class,
            0, 0);
    assertThat(committedValue).isGreaterThanOrEqualTo((long) previousValue);

    var tkmsDao = tkmsDaoProvider.getTkmsDao(0);
    var earliestMessageTracker = new EarliestMessageTracker(tkmsDao, TkmsShardPartition.of(0, 0), properties, metricsTemplate);
    earliestMessageTracker.init();

    assertThat(earliestMessageTracker.getEarliestMessageId()).isEqualTo(committedValue);
  }

  protected void sendMessageAndWaitForArrival(int cnt) {
    transactionsHelper.withTransaction().run(() ->
        tkms.sendMessage(new TkmsMessage().setTopic(testTopic).setValue("Hello Kristo!".getBytes(StandardCharsets.UTF_8)))
    );

    await().until(() -> tkmsSentMessagesCollector.getSentMessages(testTopic).size() == cnt);
  }
}
