package com.transferwise.kafka.tkms.dao;

import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.springframework.test.context.ActiveProfiles;

@TestInstance(Lifecycle.PER_CLASS)
@ActiveProfiles(profiles = {"test", "postgres"})
class PostgresTkmsDaoIntTest extends TkmsDaoIntTest {

}
