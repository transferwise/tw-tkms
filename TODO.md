# Random ideas, brainstorm

1. Add MDCs for shard and partition. Otherwise on some failure scenario, it's not clear from rollbar, which table needs help.

2. Could implement object cache for all those `TkmsShardPartition`s.

3. Avoid engineers creating tables manually. Provide at least Flyway auto configuration.

