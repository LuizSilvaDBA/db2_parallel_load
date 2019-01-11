# Overview
This shell script is used to launch multiple DB2 LOADs (different tables) at the same time. The source and target tables are mapped in the control table MIGRATION.CONTROL_LOAD. Optionally, you can do some basic filtering in the column called SOURCE_FILTER. This usefull if want to slip the load to a table into multiple partial/smaller chunks.

By using such approach all threads are kept busy as long as there are tables enough to be loaded.

The script assumes that both source and target tables have the same column list definition, so they are mapped 1-to-1. You can however overrule this behaviour by mapping the columns in the control table MIGRATION.COLUMN_MAPPING

# Control tables
Run the following to create the required control tables:
```bash
db2 -v "create table MIGRATION.CONTROL_LOAD (
   start_ts         timestamp,
   source_tabschema varchar(128) not null,
   source_tabname   varchar(128) not null,
   source_filter    varchar(200),
   target_tabschema varchar(128) not null,
   target_tabname   varchar(128) not null,
   end_ts           timestamp, 
   rows_read        bigint, 
   rows_skipped     bigint, 
   rows_loaded      bigint, 
   rows_rejected    bigint, 
   rows_deleted     bigint, 
   rows_committed   bigint, 
   load_process_id  smallint )
in <tablespace_name>"  

db2 -v "create unique index MIGRATION.UN_IN_CL on MIGRATION.CONTROL_LOAD
(
   source_tabschema asc,
   source_tabname asc,
   source_filter asc,
   target_tabschema asc,
   target_tabname asc
 )
allow reverse scans" 

db2 -v "create table MIGRATION.COLUMN_MAPPING (
   source_tabschema         varchar(128) not null,
   source_tabname           varchar(128) not null,
   source_column_expression varchar(128),
   target_tabschema         varchar(128) not null,
   target_tabname           varchar(128) not null,
   target_column            varchar(128) not null  )
in <tablespace_name>" 
 
db2 -v "create unique index MIGRATION.UN_IN_CM on MIGRATION.COLUMN_MAPPING
(
   target_tabschema asc,
   target_tabname asc,
   target_column asc
 )
allow reverse scans" 
```
A quick way to fill in the main control table is exporting from SYSCAT.TABLES in the source database (assuming there's no change inthe table's name):
```bash
# Connected to the source database
db2 "export to migration_table_list.del of DEL
select 
   NULL      as START_TS, 
   tabschema as SOURCE_TABSCHEMA, 
   tabname   as SOURCE_TABNAME, 
   NULL      as SOURCE_FILTER, 
   tabschema as TARGET_TABSCHEMA, 
   tabname   as TARGET_TABNAME, 
   NULL      as END_TS, 
   NULL      as LOAD_PROCESS_ID
from syscat.tables
where type = 'T' and tabschema = 'TEST'"

# Connected to the target database 
db2 -v "import from migration_table_list.del of DEL commitcount 1 INSERT into MIGRATION.CONTROL_LOAD" 
```

For speeding up the data transfer for big partitioned tables, you can split them into smaller tables according to their range partition definition, load them in parallel and re-attach later:
```bash
# Connected to the source database
db2 "export to migration_table_list.del of DEL
select
   NULL as START_TS,
   rtrim(dp.tabschema) as SOURCE_TABSCHEMA,  
   rtrim(dp.tabname) as SOURCE_TABNAME,  
   'where ' || rtrim(e.DATAPARTITIONEXPRESSION) as || 
   case dp.LOWINCLUSIVE
      when 'Y' then ' >= '
      when 'N' then ' > '
   end ||
   rtrim(dp.LOWVALUE) ||
   ' and ' || rtrim(e.DATAPARTITIONEXPRESSION) ||
   case dp.HIGHINCLUSIVE
      when 'Y' then ' <= '
      when 'N' then ' < '
   end ||
   rtrim(dp.HIGHVALUE) as SOURCE_FILTER,
   dp.tabname as TARGET_TABSCHEMA, -- temporarily, the table name in the source is used as table schema in the target
   rtrim(dp.DATAPARTITIONNAME) as TARGET_TABNAME, -- temporarily, the partition name in the source is used as table name in the target
   NULL as END_TS,
   NULL as LOAD_PROCESS_ID
from
   syscat.datapartitions dp join syscat.DATAPARTITIONEXPRESSION e on e.tabschema = dp.tabschema and e.tabname = dp.tabname
where
   dp.tabschema = 'TEST'"

# Connected to the target database 
db2 -v "import from migration_table_list.del of DEL commitcount 1 INSERT into MIGRATION.CONTROL_LOAD" 
```

# Usage
This script should be run as the DB2 instance id where the target database is located. The source database should be cataloged under the same instance database directory. The last argument controls the number of threads (process forks actually) that should be running at the same time:
```bash
parallel_load.sh <Source_database> <Target_database> <Number_of_parallel_loads>
``` 

# Monitoring

* Summary
```sql
select 'To be loaded', count(*) from migration.control_load 
where start_ts is null and end_ts is null and load_process_id is null
union all
select 'Completed', count(*) from migration.control_load 
where start_ts is not null and end_ts is not null and load_process_id is not null
union all
select 'Running', count(*) from migration.control_load 
where start_ts is not null and end_ts is null and load_process_id is not null
```
* Logs
```bash
tail -f *._parallel_load_from_*.out
```

