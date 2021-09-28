# Random ideas, brainstorm

1. Add MDCs for shard and partition. Otherwise on some failure scenario, it's not clear from rollbar, which table needs help.

3. Avoid engineers creating tables manually. Provide at least Flyway auto configuration.

4. Validate innodb and postgres table statistics at the start. Too many engineers have forgotten to set those correctly by docs whileas this is utmost
   important for performance.

5. Add hierarchical Valid annotations for TkmsProperties.

