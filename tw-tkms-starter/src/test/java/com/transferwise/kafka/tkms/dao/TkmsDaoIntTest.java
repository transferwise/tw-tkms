package com.transferwise.kafka.tkms.dao;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.transferwise.kafka.tkms.TkmsStorageToKafkaProxy;
import com.transferwise.kafka.tkms.api.TkmsMessage;
import com.transferwise.kafka.tkms.api.TkmsShardPartition;
import com.transferwise.kafka.tkms.test.BaseIntTest;
import com.transferwise.kafka.tkms.test.ProductionBug;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

@TestInstance(Lifecycle.PER_CLASS)
class TkmsDaoIntTest extends BaseIntTest {

  @Autowired
  private ITkmsDao tkmsDao;

  @Autowired
  private TkmsStorageToKafkaProxy tkmsStorageToKafkaProxy;

  @Autowired
  private DataSource dataSource;

  @BeforeAll
  public void setupClass() {
    tkmsStorageToKafkaProxy.pause();
    await().until(() -> tkmsStorageToKafkaProxy.isPaused());
  }

  @AfterAll
  void tearDownClass() {
    tkmsStorageToKafkaProxy.resume();
  }

  @Test
  @ProductionBug("Delete worked, but batches were combined wrongly.")
  void deletingInBatchesWorks() {
    List<Long> records = new ArrayList<>();
    for (int i = 0; i < 1001; i++) {
      records.add(
          tkmsDao.insertMessage(TkmsShardPartition.of(0, 0), new TkmsMessage().setTopic("ABC").setValue("ABC".getBytes(StandardCharsets.UTF_8)))
              .getStorageId());
    }

    assertThat(new JdbcTemplate(dataSource).queryForObject("select count(*) from outgoing_message_0_0", Integer.class)).isEqualTo(1001);

    tkmsDao.deleteMessages(TkmsShardPartition.of(0, 0), records);

    assertThat(meterRegistry.get("tw_tkms_dao_messages_delete_batches").counters()
        .stream().map(c -> c.count() * Integer.parseInt(c.getId().getTag("batchSize"))).reduce(0d, Double::sum))
        .isEqualTo(1001);

    assertThat(meterRegistry.get("tw_tkms_dao_messages_delete").counter().count()).isEqualTo(1001);

    assertDeleteBucketsCounts();

    assertThat(new JdbcTemplate(dataSource).queryForObject("select count(*) from outgoing_message_0_0", Integer.class)).isZero();
  }

  protected void assertDeleteBucketsCounts() {
    assertThat(meterRegistry.get("tw_tkms_dao_messages_delete_batches").tags("batchSize", "256").counter().count()).isEqualTo(3);
    assertThat(meterRegistry.get("tw_tkms_dao_messages_delete_batches").tags("batchSize", "64").counter().count()).isEqualTo(3);
    assertThat(meterRegistry.get("tw_tkms_dao_messages_delete_batches").tags("batchSize", "16").counter().count()).isEqualTo(2);
    assertThat(meterRegistry.get("tw_tkms_dao_messages_delete_batches").tags("batchSize", "4").counter().count()).isEqualTo(2);
    assertThat(meterRegistry.get("tw_tkms_dao_messages_delete_batches").tags("batchSize", "1").counter().count()).isEqualTo(1);
  }
}
