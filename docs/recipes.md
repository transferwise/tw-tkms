## Recipes

### Using multiple databases for different shards

Let's say you would need to use MariaDb for shard #0 and Postgres database for shard #1.

The scenario is fully supported out of the box, however you would need to provide a bean for `ITkmsDataSourceProvider`.
Letting Tkms itself to figure out which datasource is meant for which shard would be tricky, but more importantly, dangerous.

Tkms supports MySql, MariaDb and Postgres database. In a case you would need another type of database, you would also need to provide implementation
for `ITkmsDaoProvider`.