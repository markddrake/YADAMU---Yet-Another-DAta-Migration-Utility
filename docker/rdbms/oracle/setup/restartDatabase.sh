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
whenever sqlerror continue
--
-- Open any PDB whose state was not saved; ORA-65019 (already open) is expected
--
ALTER PLUGGABLE DATABASE ALL OPEN
/
exit
EOF
