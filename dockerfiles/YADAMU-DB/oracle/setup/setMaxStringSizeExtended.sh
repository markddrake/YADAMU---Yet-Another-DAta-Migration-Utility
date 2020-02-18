sqlplus -s / as sysdba <<-EOF
whenever oserror exit failure
whenever sqlerror exit failure rollback
set heading off pagesize 0 feedback off linesize 400
set trimout on trimspool on termout off echo off sqlprompt ''
--
ALTER SYSTEM SET MAX_STRING_SIZE = EXTENDED SCOPE = SPFILE
/
shutdown immediate
--
startup upgrade
--
ALTER PLUGGABLE DATABASE ALL OPEN UPGRADE
/
exit
EOF
cd $ORACLE_HOME/rdbms/admin
mkdir -p /tmp/utl32k_cdb_pdbs_output
$ORACLE_HOME/perl/bin/perl $ORACLE_HOME/rdbms/admin/catcon.pl -u SYS/oracle -d $ORACLE_HOME/rdbms/admin -l '/tmp/utl32k_cdb_pdbs_output' -b utl32k_cdb_pdbs_output utl32k.sql
sqlplus -s / as sysdba <<-EOF
whenever oserror exit failure
whenever sqlerror exit failure rollback
set heading off pagesize 0 feedback off linesize 400
set trimout on trimspool on termout off echo off sqlprompt ''
--
shutdown immediate
--
startup
--
exit
EOF
