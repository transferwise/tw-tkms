package com.transferwise.kafka.tkms;

import static org.assertj.core.api.Assertions.assertThat;

import com.transferwise.kafka.tkms.api.TkmsShardPartition;
import com.transferwise.kafka.tkms.config.ITkmsKafkaProducerProvider;
import com.transferwise.kafka.tkms.config.ITkmsKafkaProducerProvider.UseCase;
import com.transferwise.kafka.tkms.test.BaseIntTest;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

class KafkaMetricsIntTest extends BaseIntTest {


  @Autowired
  private ITkmsKafkaProducerProvider tkmsKafkaProducerProvider;

  @Test
  @SneakyThrows
  void testThatProducerMetricShowsSentMessage() {
    var producer = tkmsKafkaProducerProvider.getKafkaProducer(TkmsShardPartition.of(0, 0), UseCase.TEST);

    producer.send(new ProducerRecord<>(testProperties.getTestTopic(), new byte[]{})).get(5, TimeUnit.SECONDS);

    assertThat(getAccumulativeCount("kafka.producer.record.send.total"))
        .as("Producer's metric shows one message sent.").isPositive();
  }

  protected double getAccumulativeCount(String metricsName) {
    double sum = 0d;
    for (var counter : meterRegistry.find(metricsName).functionCounters()) {
      sum += counter.count();
    }
    return sum;
  }
}
