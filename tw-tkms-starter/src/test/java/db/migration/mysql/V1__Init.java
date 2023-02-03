package db.migration.mysql;

import java.sql.Statement;
import lombok.extern.slf4j.Slf4j;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;

@Slf4j
public class V1__Init extends BaseJavaMigration {

  @Override
  public void migrate(Context context) throws Exception {
    for (int s = 0; s < 5; s++) {
      for (int p = 0; p < 10; p++) {
        try (Statement stmt = context.getConnection().createStatement()) {
          String tableName = "outgoing_message_" + s + "_" + p;
          stmt.execute("CREATE TABLE " + tableName + " (\n"
              + "  id BIGINT AUTO_INCREMENT PRIMARY KEY,\n"
              + "  message MEDIUMBLOB NOT NULL\n"
              + ")  stats_persistent=1, stats_auto_recalc=0 ENGINE=InnoDB");
          log.info("Created table `" + tableName + "'.");

          stmt.execute(
              "update mysql.innodb_index_stats set stat_value=1000000 where table_name = \"" + tableName + "\" and stat_description=\"id\";");
          stmt.execute("update mysql.innodb_table_stats set n_rows=1000000 where table_name like \"" + tableName + "\";");
          stmt.execute("insert into mysql.table_stats (db_name, table_name, cardinality) values(DATABASE(), \"" + tableName
              + "\", 1000000) on duplicate key update cardinality=1000000");
        }
      }
    }

    try (Statement stmt = context.getConnection().createStatement()) {
      stmt.execute("flush tables");
    }
  }
}
