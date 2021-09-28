CREATE TABLE complex_test_messages
(
    id         BIGINT AUTO_INCREMENT PRIMARY KEY,
    entity_id  BIGINT       NOT NULL,
    entity_seq BIGINT       NOT NULL,
    topic      VARCHAR(100) NOT NULL
);