package com.transferwise.kafka.tkms.demoapp.complextest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.transferwise.common.baseutils.ExceptionUtils;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class ComplexTestMessagesListener {

  @Autowired
  private JdbcTemplate jdbcTemplate;
  @Autowired
  private ObjectMapper objectMapper;

  @KafkaListener(topics = {"ComplexTest0", "ComplexTest1", "ComplexTest2", "ComplexTest3", "ComplexTest4"})
  @SneakyThrows
  public void listen1(List<ConsumerRecord<String, byte[]>> consumerRecords) {
    jdbcTemplate.batchUpdate("INSERT INTO complex_test_messages (topic, entity_id, entity_seq) values (?,?,?)", new BatchPreparedStatementSetter() {
      @Override
      public void setValues(PreparedStatement ps, int i) throws SQLException {
        ConsumerRecord<String, byte[]> consumerRecord = consumerRecords.get(i);
        ComplexTestMessage complexTestMessage = ExceptionUtils.doUnchecked(()->objectMapper.readValue(consumerRecord.value(), ComplexTestMessage.class));
        ps.setString(1, consumerRecord.topic());
        ps.setLong(2, complexTestMessage.getEntityId());
        ps.setLong(3, complexTestMessage.getEntitySeq());
      }

      @Override
      public int getBatchSize() {
        return consumerRecords.size();
      }
    });
  }
}
