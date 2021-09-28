CREATE TABLE complex_test_messages
(
    id BIGSERIAL PRIMARY KEY,
    entity_id  BIGINT NOT NULL,
    entity_seq BIGINT NOT NULL,
    topic      TEXT   NOT NULL
);
