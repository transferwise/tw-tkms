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
              + ")  ENGINE=InnoDB PAGE_COMPRESSED=1");
          log.info("Create table `" + tableName + "'.");
        }
      }
    }
  }
}
