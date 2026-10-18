export PATH=$PATH:/opt/mssql-tools/bin:/opt/mssql-tools18/bin
export MSSQL_VERSION=$1
which sqlcmd
export STAGE=/var/opt/mssql/stage
mkdir -p $STAGE/log
cd $STAGE
export DB_USER=sa
export DB_PWD=oracle#1
export DB_HOST=localhost
# Configure Max Memory 12G
export MSSQL_MEMORY=12288
sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -dmaster -I -C -e -i setup/configure.sql > log/configure.log 2>&1
# Install YADAMU_IMPORT
sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -dmaster -I -C -e -i sql/YADAMU_IMPORT.sql > log/YADAMU_IMPORT.log 2>&1
cat log/YADAMU_IMPORT.log
# Install YADAMU_COMPARE
sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -dmaster -I -C -e -i sql/YADAMU_COMPARE.sql   > log/YADAMU_COMPARE.log 2>&1
cat log/YADAMU_COMPARE.log
# Install Northwind
sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -dmaster -I -C -e -i testdata/Northwind/instnwnd.sql > log/instnwnd.log 2>&1
# Restore a sample database with setup/<name>.sql, then check that it is ONLINE.
# SQL Server occasionally fails to recover a restore (Msg 3414 on WorldWideImportersDW's In-Memory OLTP filegroup, 2022-latest, 2026-10-08) and a
# second attempt succeeds. Each script drops the database first, so it can simply be rerun. The second attempt is logged to log/<name>.retry.log.
RESTORE_FAILURES=0
restoreDatabase() {
  local DB_NAME=$1
  local LOG_FILE=log/$DB_NAME.log
  for ATTEMPT in 1 2; do
    sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -dmaster -I -C -e -i setup/$DB_NAME.sql -v MSSQL_VERSION=$MSSQL_VERSION > $LOG_FILE 2>&1
    cat $LOG_FILE
    local DB_STATE=$(sqlcmd -U$DB_USER -P$DB_PWD -S$DB_HOST -dmaster -C -h-1 -W -Q "set nocount on; select state_desc from sys.databases where name = '$DB_NAME'")
    if [ "$DB_STATE" = "ONLINE" ]; then
      return 0
    fi
    echo "Restore of $DB_NAME failed on attempt $ATTEMPT. Database state: ${DB_STATE:-not found}."
    LOG_FILE=log/$DB_NAME.retry.log
  done
  echo "ERROR: $DB_NAME is not ONLINE after 2 attempts."
  RESTORE_FAILURES=$((RESTORE_FAILURES+1))
}
# Install AdventureWorks
restoreDatabase AdventureWorks
# Install AdventureWorks Data Warehouse
restoreDatabase AdventureWorksDW
# Install Wide World Importers
restoreDatabase WorldWideImporters
# Install Wide World Importers Data Warehouse
restoreDatabase WorldWideImportersDW
if [ $RESTORE_FAILURES -gt 0 ]; then
  echo "ERROR: $RESTORE_FAILURES sample database restores failed."
  exit 1
fi
