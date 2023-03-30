# Random ideas, brainstorm

3. Avoid engineers creating tables manually. Provide at least Flyway auto configuration.

5. Add hierarchical Valid annotations for TkmsProperties.

6. It turns out, than in the polling cycle, the sending messages to the Kafka can be by far the slowest step,
looking at 95th percentiles. We need to figure out, how to overcome this.
* producer per partition?
* proper producer configuration - large enough batch sizes, lingering, in flight requests count etc.

