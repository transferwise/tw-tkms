package com.transferwise.kafka.tkms.dao;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.springframework.test.context.ActiveProfiles;

@TestInstance(Lifecycle.PER_CLASS)
@ActiveProfiles(profiles = {"test", "postgres"})
class PostgresTkmsDaoIntTest extends TkmsDaoIntTest {

  /*
    Here we test if `delete-batch-sizes` custom configuration applies.
   */
  @Override
  protected void assertDeleteBucketsCounts() {
    assertThat(meterRegistry.get("tw_tkms_dao_messages_delete").tags("batchSize", "51").counter().count()).isEqualTo(19);
    assertThat(meterRegistry.get("tw_tkms_dao_messages_delete").tags("batchSize", "11").counter().count()).isEqualTo(2);
    assertThat(meterRegistry.get("tw_tkms_dao_messages_delete").tags("batchSize", "5").counter().count()).isEqualTo(2);
  }

}
