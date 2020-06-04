package com.transferwise.kafka.tkms.demoapp.complextest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.transferwise.kafka.tkms.api.ITransactionalKafkaMessageSender;
import com.transferwise.kafka.tkms.api.Message;
import lombok.SneakyThrows;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ComplexTestController {

  @Autowired
  private ITransactionalKafkaMessageSender tkms;
  @Autowired
  private ObjectMapper objectMapper;
  @Autowired
  private JdbcTemplate jdbcTemplate;

  @PostMapping("/complexTest/reset")
  public void reset() {
    jdbcTemplate.update("TRUNCATE complex_test_messages");
  }

  @PostMapping("/complexTest/produceSpamMessages")
  @SneakyThrows
  public Long addMessage(@RequestParam String topic, @RequestParam long entityId, @RequestParam long entitySeq) {
    ComplexTestMessage complexTestMessage = new ComplexTestMessage().setTopic(topic).setEntityId(entityId).setEntitySeq(entitySeq);
    return tkms.sendMessage(new Message().setTopic(topic).setKey(String.valueOf(entityId))
        .setValue(objectMapper.writeValueAsBytes(complexTestMessage)));
  }

  @PostMapping("/complexTest/getRecordedMessagesCount")
  public Long getRecordedMessagesCount() {
    return jdbcTemplate.queryForObject("select count(*) from complex_test_messages", Long.class);
  }

}
