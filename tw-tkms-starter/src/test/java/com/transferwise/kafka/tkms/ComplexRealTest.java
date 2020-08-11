package com.transferwise.kafka.tkms;

import static org.awaitility.Awaitility.await;

import com.transferwise.common.baseutils.ExceptionUtils;
import java.net.URI;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.RequestEntity;
import org.springframework.web.client.RestTemplate;

@Slf4j
@Disabled("Not meant to be automatically run.")
public class ComplexRealTest {

  private final RestTemplate restTemplate = new RestTemplate();

  /**
   * Runs 135s on my laptop with 5 * 10000 * 5.
   */
  @Test
  @SneakyThrows
  void complexTest() {
    ExecutorService executorService = Executors.newFixedThreadPool(10);
    long topicsCount = 5;
    long entitiesCount = 10_000;
    long entityEventsCount = 5;

    rpc("/complexTest/reset");

    final long start = System.currentTimeMillis();

    for (long t = 0; t < topicsCount; t++) {
      for (long e = 0; e < entitiesCount; e++) {
        long finalE = e;
        long finalT = t;
        executorService.submit(() -> {
          for (long ev = 0; ev < entityEventsCount; ev++) {
            try {
              rpc("/complexTest/produceSpamMessages?topic=ComplexTest" + finalT + "&entityId=" + finalE + "&entitySeq=" + ev);
            } catch (Throwable error) {
              log.error(error.getMessage(), error);
            }
          }
        });
      }
    }

    executorService.shutdown();
    if (!executorService.awaitTermination(1, TimeUnit.HOURS)) {
      throw new IllegalStateException("Did not terminate properly.");
    }

    await().atMost(Duration.ofHours(1)).pollInterval(Duration.ofSeconds(5)).until(() ->
        Long.parseLong(rpc("/complexTest/getRecordedMessagesCount")) >= topicsCount * entitiesCount * entityEventsCount);

    log.info("Done in " + (System.currentTimeMillis() - start) + " ms.");
  }

  private String rpc(String url) {
    int[] ports = new int[]{8080, 8081};
    int portIdx = ThreadLocalRandom.current().nextInt(ports.length);

    int tries = 0;
    while (tries < ports.length) {
      try {
        int port = ports[portIdx];

        HttpHeaders headers = new HttpHeaders();
        headers.add(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE);

        URI uri = ExceptionUtils.doUnchecked(() -> new URI("http://localhost:" + port + url));
        RequestEntity<String> requestEntity = new RequestEntity<>(headers, HttpMethod.POST, uri);

        return restTemplate.exchange(requestEntity, String.class).getBody();
      } catch (Exception e) {
        if (tries++ >= ports.length) {
          throw e;
        }
        log.error(e.getMessage());
        portIdx++;
        if (portIdx >= ports.length) {
          portIdx = 0;
        }
      }
    }
    throw new IllegalStateException("Can not reach here.");
  }
}
