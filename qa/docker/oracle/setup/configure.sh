mkdir -p /opt/oracle/oradata/admin/$ORACLE_SID/adump
mkdir -p /opt/oracle/oradata/flash_recovery_area
mkdir -p /opt/oracle/oradata/diag
sqlplus sys/oracle@$ORACLE_PDB as sysdba @/opt/oracle/oradata/setup/configure.sql
unzip testdata/sampleSchemas.zip
cd /opt/oracle/oradata/sampleSchemas
sqlplus /nolog @mksample oracle oracle oracle oracle oracle oracle oracle oracle USERS TEMP log/ $ORACLE_PDB
