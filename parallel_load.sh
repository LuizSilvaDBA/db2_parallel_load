#!/bin/bash
   
#------------------------------------
# Input validation
#------------------------------------

# 1) number number of arguments

if [ "$#" == "3" ]
then
   export SOURCE_DATABASE=`echo $1 | tr '[a-z]' '[A-Z]'`
   export TARGET_DATABASE=`echo $2 | tr '[a-z]' '[A-Z]'`
   export NUMBER_OF_INSTANCES=$3
else
   echo "Error! Insufficient number of arguments..."
   echo "Usage: $0 <Source_database> <Target_database> <Number_of_parallel_loads>"
   echo ""
   exit 1
fi

# 2) check if the specified SOURCE database (remotely cataloged) exist

DB_EXIST=`db2 list db directory  | awk -v a=${SOURCE_DATABASE} '
/Database alias/{
   dbname = $4
}
 
/Directory entry type/{
   entrytype=$5 ; 
   if( (entrytype == "Remote") && (dbname == a ) ){ 
      print dbname
   }
}'`

if [ "${DB_EXIST}" != "${SOURCE_DATABASE}" ] ; then
   echo "The source database ${SOURCE_DATABASE} was not cataloged. Leaving..."
   exit 1
fi

# 3) check if the specified TARGET database (local) exist

DB_EXIST=`db2 list db directory  | awk -v a=${TARGET_DATABASE} '
/Database alias/{
   dbname = $4
}
 
/Directory entry type/{
   entrytype=$5 ; 
   if( (entrytype == "Indirect") && (dbname == a ) ){ 
      print dbname
   }
}'`

if [ "${DB_EXIST}" != "${TARGET_DATABASE}" ] ; then
   echo "The target database ${TARGET_DATABASE} was not found. Leaving..."
   exit 1
fi

#------------------------------------
# Functions
#------------------------------------


create_load_instance ()
{
   LOAD_ID=$1
   LOAD_LOG=${LOG_TIMESTAMP}_parallel_load_from_${SOURCE_DATABASE}_to_${TARGET_DATABASE}_${LOAD_ID}.out
   END_OF_TABLE=0
 
   echo "Load thread ${LOAD_ID} started at `date`"         >> $LOAD_LOG
   echo "SHELL PID: $$"                                    >> $LOAD_LOG
   echo ""                                                 >> $LOAD_LOG


   while [ $END_OF_TABLE -ne 1 ]
   do 
      echo "-------------------------------------------------------------------------------------------------------------------" >> $LOAD_LOG
      db2 -v "connect to ${TARGET_DATABASE}" >> $LOAD_LOG  
      db2 -x +v "values('Starting at:', current timestamp)" >> $LOAD_LOG
      echo "" >> $LOAD_LOG
   
      # make sure only 1 single thread is able to get the next available table 
      db2 +c -v "lock table migration.control_load in exclusive mode" >> $LOAD_LOG
   
      # pick the next available table 
      FULL_RECORD_SELECTED=$( db2 -x +v +c "select rtrim(target_tabschema) || ':' || rtrim(target_tabname) || ':' || rtrim(source_tabschema) || ':' || rtrim(source_tabname) || ':' || rtrim(coalesce(source_filter, 'NULL')) || ':' || rtrim(coalesce(char(load_process_id), ' ')) from migration.control_load where start_ts is null and end_ts is null and load_process_id is null fetch first 1 row only" )
   
      if [ "${FULL_RECORD_SELECTED}" == "" ];
      then
         END_OF_TABLE=1
         db2 -v "commit" >> $LOAD_LOG
         echo "Load thread ${LOAD_ID} finished at `date`" >> $LOAD_LOG
      else
         TARGET_TABSCHEMA=`echo ${FULL_RECORD_SELECTED} | awk 'BEGIN{FS=":"}{print $1}'`
         TARGET_TABNAME=`  echo ${FULL_RECORD_SELECTED} | awk 'BEGIN{FS=":"}{print $2}'`
         SOURCE_TABSCHEMA=`echo ${FULL_RECORD_SELECTED} | awk 'BEGIN{FS=":"}{print $3}'`
         SOURCE_TABNAME=`  echo ${FULL_RECORD_SELECTED} | awk 'BEGIN{FS=":"}{print $4}'`
         OPTIONAL_FILTER=` echo ${FULL_RECORD_SELECTED} | awk 'BEGIN{FS=":"}{print $5}'` 
         LOAD_PROCESS_ID=` echo ${FULL_RECORD_SELECTED} | awk 'BEGIN{FS=":"}{print $6}'`
      
         echo "Table selected: ${SOURCE_TABSCHEMA}.${SOURCE_TABNAME} -> ${TARGET_TABSCHEMA}.${TARGET_TABNAME}"  >> $LOAD_LOG 
         echo "" >> $LOAD_LOG  
 
         db2 -v +c -m "update migration.control_load set start_ts = current timestamp, load_process_id = ${LOAD_ID} where start_ts is null and end_ts is null and target_tabschema = '${TARGET_TABSCHEMA}' and target_tabname = '${TARGET_TABNAME}'" >> $LOAD_LOG
         db2 -v "commit" >> $LOAD_LOG 
   
         START_TS=$(db2 -x +v "select start_ts from migration.control_load where source_tabschema = '${SOURCE_TABSCHEMA}' and source_tabname = '${SOURCE_TABNAME}' and load_process_id = ${LOAD_ID} and target_tabschema = '${TARGET_TABSCHEMA}' and target_tabname = '${TARGET_TABNAME}'") 
         echo -e "Start timestamp: ${START_TS} \n" >> $LOAD_LOG 
   
         COLUMN_MAPPING_COUNT=`db2 connect to ${TARGET_DATABASE} >/dev/null; db2 -x "select count(*) from MIGRATION.COLUMN_MAPPING where TARGET_TABSCHEMA = '${TARGET_TABSCHEMA}' and TARGET_TABNAME = '${TARGET_TABNAME}' with ur" | awk '{print}'`
   
         # check if there's any mapping defined
         if [ $COLUMN_MAPPING_COUNT -gt 0 ] 
         then 
            # use the column mapping defined in MIGRATION.COLUMN_MAPPING
            TARGET_COLUMN_LIST=`db2 connect to ${TARGET_DATABASE} >/dev/null; db2 -x "select LISTAGG(m.SOURCE_COLUMN_EXPRESSION, ' ,') within group(order by c.colno asc) from MIGRATION.COLUMN_MAPPING m join syscat.columns c on m.TARGET_TABSCHEMA = c.TABSCHEMA and m.TARGET_TABNAME = c.TABNAME and m.TARGET_COLUMN = c.COLNAME where m.TARGET_TABSCHEMA = '${TARGET_TABSCHEMA}' and m.TARGET_TABNAME = '${TARGET_TABNAME}' with ur"` 
         else    
            # use the same column list present in the target table (1 to 1 data move)
            TARGET_COLUMN_LIST=`db2 connect to ${TARGET_DATABASE} >/dev/null; db2 -x "select '\"' || rtrim(colname) || '\"' from syscat.columns where tabschema = '${TARGET_TABSCHEMA}' and tabname = '${TARGET_TABNAME}' order by colno with ur" | tr -d ' ' | tr '\n' ',' | head -c -1` 
         fi
   
         IDENTITYOVERRIDE_FLAG=`db2 connect to ${TARGET_DATABASE} >/dev/null; db2 -x "select 'identityoverride' from syscat.columns where tabschema = '${TARGET_TABSCHEMA}' and tabname = '${TARGET_TABNAME}' and identity = 'Y' and generated = 'A' fetch first 1 row only with ur"`
         GENERATEDOVERRIDE_FLAG=`db2 connect to ${TARGET_DATABASE} >/dev/null; db2 -x "select 'generatedoverride' from syscat.columns where tabschema = '${TARGET_TABSCHEMA}' and tabname = '${TARGET_TABNAME}' and identity = 'N' and generated = 'A' fetch first 1 row only with ur"`
         ROWCHANGETIMESTAMPOVERRIDE_FLAG=`db2 connect to ${TARGET_DATABASE} >/dev/null; db2 -x "select 'rowchangetimestampoverride' from syscat.columns where tabschema = '${TARGET_TABSCHEMA}' and tabname = '${TARGET_TABNAME}' and identity = 'N' and generated = 'A' and rowchangetimestamp = 'Y' fetch first 1 row only with ur"`          
        
         # in case you want to enable compression to all tables:
		 # db2 -v "alter table ${TARGET_TABSCHEMA}.${TARGET_TABNAME} compress yes ADAPTIVE" >> $LOAD_LOG
   
         # check if directory for message files exists
         if [ ! -d ./message_files ]; then
            mkdir ./message_files
         fi
 
         # make sure the shell variable OPTIONAL_FILTER will only be filled with a real where clause filtering, not with the string "NULL" or empty ""
         if [ "${OPTIONAL_FILTER}" == "NULL" ] || [ "${OPTIONAL_FILTER}" == "" ]; then 
            OPTIONAL_FILTER=""
         fi 
   
         # CURSOR definition
         db2 -v "declare C1 cursor database ${SOURCE_DATABASE} user ${SOURCE_USER} using '${SOURCE_USER_PW}' for select ${TARGET_COLUMN_LIST} from ${SOURCE_TABSCHEMA}.${SOURCE_TABNAME} ${OPTIONAL_FILTER} with UR" >> $LOAD_LOG
         echo "" >> $LOAD_LOG

         # unique temp file used to extract the number of rows read & commited after the load
         TMP_LOAD_OUTPUT=$(mktemp ./temp.XXXXXX)
   
         # LOAD command. Special remark: data buffer = 25600 x 4 KB = 100 MB
         db2 -z $TMP_LOAD_OUTPUT -v "load from C1 of cursor modified by anyorder ${IDENTITYOVERRIDE_FLAG} ${GENERATEDOVERRIDE_FLAG} ${ROWCHANGETIMESTAMPOVERRIDE_FLAG} messages message_files/${TARGET_TABSCHEMA}.${TARGET_TABNAME}.msg insert into ${TARGET_TABSCHEMA}.${TARGET_TABNAME} nonrecoverable data buffer 25600" >> $LOAD_LOG
         RC=$?
   
         if [ "${RC}" == "0" ] 
         then 
            ROWS_READ=`     awk '/Number of rows read/      {print $6}' $TMP_LOAD_OUTPUT` 
            ROWS_SKIPPED=`  awk '/Number of rows skipped/   {print $6}' $TMP_LOAD_OUTPUT` 
            ROWS_LOADED=`   awk '/Number of rows loaded/    {print $6}' $TMP_LOAD_OUTPUT` 
            ROWS_REJECTED=` awk '/Number of rows rejected/  {print $6}' $TMP_LOAD_OUTPUT` 
            ROWS_DELETED=`  awk '/Number of rows deleted/   {print $6}' $TMP_LOAD_OUTPUT` 
            ROWS_COMMITTED=`awk '/Number of rows committed/ {print $6}' $TMP_LOAD_OUTPUT` 
   
            # it's a bit ugly, but that's the simpliest way to handle escape of single quotes(') character 
            OPTIONAL_FILTER=`echo $OPTIONAL_FILTER | sed "s/'/''/g" ` 
            echo "" >> $LOAD_LOG
   
            db2 -v -m "update migration.control_load set end_ts = current timestamp, rows_read = ${ROWS_READ}, rows_skipped = ${ROWS_SKIPPED}, rows_loaded = ${ROWS_LOADED}, rows_rejected = ${ROWS_REJECTED}, rows_deleted = ${ROWS_DELETED}, rows_committed = ${ROWS_COMMITTED} where start_ts = '${START_TS}' and source_tabschema = '${SOURCE_TABSCHEMA}' and source_tabname = '${SOURCE_TABNAME}' and target_tabschema = '${TARGET_TABSCHEMA}' and target_tabname = '${TARGET_TABNAME}'" >> $LOAD_LOG
            echo "" >> $LOAD_LOG
   
            END_TS=$(db2 -x +v "select end_ts from migration.control_load where start_ts = '${START_TS}' and source_tabschema = '${SOURCE_TABSCHEMA}' and source_tabname = '${SOURCE_TABNAME}' and load_process_id = ${LOAD_ID} and target_tabschema = '${TARGET_TABSCHEMA}' and target_tabname = '${TARGET_TABNAME}'")
            echo -e "End timestamp: ${END_TS} \n" >> $LOAD_LOG
   
            db2 -v "connect reset" > /dev/null 
         else 
            echo "" >> $LOAD_LOG
            echo "Load from ${SOURCE_TABSCHEMA}.${SOURCE_TABNAME} into ${TARGET_TABSCHEMA}.${TARGET_TABNAME} failed!" >> $LOAD_LOG   
         fi 
   
         rm -f $TMP_LOAD_OUTPUT 
  
   
      fi   # end of FULL_RECORD_SELECTED IF statement
   
   echo "" >> $LOAD_LOG
        
   done # end of while  
}


#---------------------
# Main body
#---------------------

export LOG_TIMESTAMP=`date +%Y%m%d%H%M%S`

echo "User to connect to ${SOURCE_DATABASE}: "
read SOURCE_USER
export SOURCE_USER

echo "${SOURCE_USER}'s password: "
read -s SOURCE_USER_PW
export SOURCE_USER

# Test connection to the Source database
db2 -v "connect to ${SOURCE_DATABASE} user ${SOURCE_USER} using '${SOURCE_USER_PW}'" 
DB2_RC=$?

if [ $DB2_RC -eq 0 ]
then 
   echo "Connect to ${SOURCE_DATABASE}: OK"
   db2 connect reset > /dev/null
else 
   echo "Failure to connect to ${SOURCE_DATABASE} with user ${SOURCE_USER}"
   echo "Return code: ${DB2_RC}"
   echo "Leaving..."
   exit 1 
fi 


for i in $( seq 1 $NUMBER_OF_INSTANCES )
do 
   echo "Creating load instance $i"
   ( create_load_instance $i ) & 
done