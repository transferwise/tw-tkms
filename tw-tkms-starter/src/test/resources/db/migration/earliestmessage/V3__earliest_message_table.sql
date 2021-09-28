CREATE TABLE earliestmessage.tw_tkms_earliest_visible_messages
(
    shard      BIGINT NOT NULL,
    part       BIGINT NOT NULL,
    message_id BIGINT NOT NULL,
    PRIMARY KEY (shard, part)
) 