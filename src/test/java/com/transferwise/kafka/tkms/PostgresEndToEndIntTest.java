package com.transferwise.kafka.tkms;

import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles(profiles = {"test", "postgres"})
public class PostgresEndToEndIntTest  extends EndToEndIntTest {

}
