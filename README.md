# tw-tkms

MariaDb:
auto increment: 2

useCompression=true results better performance on large (100k+ messages).
`ENGINE=InnoDB PAGE_COMPRESSED=1` on table makes it even faster.

Manual index stats
https://www.percona.com/blog/2017/09/11/updating-innodb-table-statistics-manually/


update mysql.innodb_index_stats set stat_value=100000 where table_name="outgoing_message_1_1" and stat_description="id";
update mysql.innodb_table_stats set n_rows=100000 where table_name="outgoing_message_1_1";
 

alter table outgoing_message_0_0 stats_persistent=1, stats_auto_recalc=0;
alter table outgoing_message_0_1 stats_persistent=1, stats_auto_recalc=0;
alter table outgoing_message_1_0 stats_persistent=1, stats_auto_recalc=0;
alter table outgoing_message_1_1 stats_persistent=1, stats_auto_recalc=0;

update mysql.innodb_index_stats set stat_value=100000 where table_name like "outgoing_message%" and stat_description="id";
update mysql.innodb_table_stats set n_rows=100000 where table_name like "outgoing_message%";

flush table outgoing_message_0_0;
flush table outgoing_message_0_1;
flush table outgoing_message_1_0;
flush table outgoing_message_1_1;

alter table ninjas_outgoing_message_0_0 stats_persistent=1, stats_auto_recalc=0, ALGORITHM=INSTANT, LOCK=NONE;
alter table ninjas_outgoing_message_0_1 stats_persistent=1, stats_auto_recalc=0, ALGORITHM=INSTANT, LOCK=NONE;
alter table ninjas_outgoing_message_1_0 stats_persistent=1, stats_auto_recalc=0, ALGORITHM=INSTANT, LOCK=NONE;
alter table ninjas_outgoing_message_1_1 stats_persistent=1, stats_auto_recalc=0, ALGORITHM=INSTANT, LOCK=NONE;

alter table public_outgoing_message_0_0 stats_persistent=1, stats_auto_recalc=0, ALGORITHM=INSTANT, LOCK=NONE;
alter table public_outgoing_message_0_1 stats_persistent=1, stats_auto_recalc=0, ALGORITHM=INSTANT, LOCK=NONE;
alter table public_outgoing_message_1_0 stats_persistent=1, stats_auto_recalc=0, ALGORITHM=INSTANT, LOCK=NONE;
alter table public_outgoing_message_1_1 stats_persistent=1, stats_auto_recalc=0, ALGORITHM=INSTANT, LOCK=NONE;

update mysql.innodb_index_stats set stat_value=100000 where table_name like "ninjas_outgoing_message%" and stat_description="id";
update mysql.innodb_index_stats set stat_value=100000 where table_name like "public_outgoing_message%" and stat_description="id";

update mysql.innodb_table_stats set n_rows=100000 where table_name like "ninjas_outgoing_message%";
update mysql.innodb_table_stats set n_rows=100000 where table_name like "public_outgoing_message%";

set lock_wait_timeout=1;
flush table ninjas_outgoing_message_0_0;
flush table ninjas_outgoing_message_0_1;
flush table ninjas_outgoing_message_1_0;
flush table ninjas_outgoing_message_1_1;

flush table public_outgoing_message_0_0;
flush table public_outgoing_message_0_1;
flush table public_outgoing_message_1_0;
flush table public_outgoing_message_1_1;
