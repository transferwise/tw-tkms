package db.migration.earliestmessage;

import java.sql.Statement;
import lombok.extern.slf4j.Slf4j;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;

@Slf4j
public class V2__Init extends BaseJavaMigration {

  @Override
  public void migrate(Context context) throws Exception {
    for (int s = 0; s < 5; s++) {
      for (int p = 0; p < 10; p++) {
        Statement stmt = context.getConnection().createStatement();
        try {
          String tableName = "earliestmessage.outgoing_message_" + s + "_" + p;
          stmt.execute("CREATE TABLE " + tableName + " (\n"
              + "  id BIGSERIAL PRIMARY KEY,\n"
              + "  message BYTEA NOT NULL\n"
              + ") WITH (autovacuum_analyze_threshold=1000000000, autovacuum_vacuum_threshold=100000, toast_tuple_target=8160) ");
          log.info("Create table `" + tableName + "'.");

          stmt.executeUpdate("ALTER TABLE " + tableName + " ALTER COLUMN id SET (n_distinct=1000000);");
          stmt.executeUpdate("ALTER TABLE " + tableName + " ALTER COLUMN message SET STORAGE EXTERNAL");
          stmt.executeUpdate("VACUUM FULL " + tableName);
        }
        finally{
          stmt.close();
        }
      }
    }
  }

  @Override
  public boolean canExecuteInTransaction() {
    return false;
  }
}
