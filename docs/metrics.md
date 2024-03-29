# Metrics

The library provides out of the box instrumentation via Micrometer.

Notation used: `metric_name {tag1, tag2, tag3...}`

## tw-tkms-starter

### Counters

`tw_tkms_proxy_message_send {shard, partition, topic, success}`

`tw_tkms_interface_message_registration {shard, partition, epName, epGroup, epOwner, topic}`

`tw_tkms_dao_message_insert {shard, partition, epName, epGroup, epOwner, topic} `

### Gauges

`tw_library_info {library, version}`

`tw_tkms_dao_earliest_message_id {shard, partition}`

`tw_tkms_dao_rows_in_table_stats {shard, partition}`

`tw_tkms_dao_rows_in_index_stats {shard, partition}`

`tw_tkms_dao_approximate_messages_count {shard, partition}`

### Summaries

`tw_tkms_dao_poll_all_results_count {shard, partition}`

### Timers

`tw_tkms_proxy_poll {shard, partition, pollResult}`

`tw_tkms_message_insert_to_ack {shard, partition, topic}`

`tw_tkms_dao_poll_first_result {shard, partition}`

`tw_tkms_dao_poll_all_results {shard, partition}`

`tw_tkms_dao_poll_get_connection {shard, partition}`

`tw_tkms_proxy_cycle {shard, partition, pollResult}`

`tw_tkms_proxy_kafka_messages_send {shard, partition}`

`tw_tkms_proxy_messages_delete {shard, partition}`

`tw_tkms_dao_messages_delete {shard, partition, batchSize}`

`tw_tkms_stored_message_parsing {shard, partition}`

`tw_tkms_dao_serialization_original_size_bytes {shard, partition, algorithm}`

`tw_tkms_dao_serialization_serialized_size_bytes {shard, partition, algorithm}`

`tw_tkms_dao_serialization_compression_ratio {shard, partition, algorithm}`

`tw_tkms_dao_insert_invalid_generated_keys_count {shard, partition}`

### Tags

| Tag          | Description                                                               |
|--------------|---------------------------------------------------------------------------|
| shard        | Shard                                                                     |
| partition    | TwTkms partition (not Kafka's)                                            |
| epName       | Entry point name                                                          |
| epGroup      | Entry point group                                                         |
| epOwner      | Entry point owner                                                         |
| version      | Library version                                                           |
| library      | Constant 'tw-tkms'                                                        |
| topic        | Kafka topic                                                               |
| success      | true/false                                                                |
| batchSize    | batch size for the operation                                              |
| pollResult   | 'empty', 'not_empty'                                                      |
| algorithm    | 'none', 'snappy', 'snappy_framed', 'zstd'                                 |