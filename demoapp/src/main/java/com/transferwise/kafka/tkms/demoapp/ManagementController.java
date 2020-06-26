package com.transferwise.kafka.tkms.demoapp;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ManagementController {

  @Autowired
  private MessagesProducer producer;

  // curl -X POST "localhost:8080/produce?threadCount=20&batchCount=1000&batchSize=100"

  @PostMapping("/produce")
  public void produce(@RequestParam long threadCount, @RequestParam long batchCount, @RequestParam long batchSize) {
    producer.produce(threadCount, batchCount, batchSize);
  }

}
