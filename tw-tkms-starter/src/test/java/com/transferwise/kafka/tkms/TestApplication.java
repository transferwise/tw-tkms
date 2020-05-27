package com.transferwise.kafka.tkms;

import com.transferwise.common.baseutils.transactionsmanagement.TransactionsConfiguration;
import javax.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;

@SpringBootApplication
@Import(TransactionsConfiguration.class)
@Slf4j
public class TestApplication {

  @PostConstruct
  public void init() {
    log.info("Starting Test Application.");
  }
}
