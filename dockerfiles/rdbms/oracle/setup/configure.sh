mkdir -p /opt/oracle/oradata/flash_recovery_area
sqlplus sys/oracle@$ORACLE_PDB as sysdba @/opt/oracle/diag/setup/configure.sql
echo "# Do Nothing" > extendedStringSizeAction.sh
sqlplus -s / as sysdba <<-EOF
whenever oserror exit failure
whenever sqlerror exit failure rollback
set heading off pagesize 0 feedback off linesize 400
set trimout on trimspool on termout off echo off sqlprompt ''
ALTER SESSION SET CONTAINER = CDB\$ROOT
/
VAR UTL32K_ACTION VARCHAR2(1024)
--
begin
  select case 
           when VALUE = 'EXTENDED' then
	   	     '# MAX_STRNG_SIZE = EXTENDED'
	       else
		     'source /opt/oracle/diag/setup/setMaxStringSizeExtended.sh'
         end SET_32K_SCRIPT 
    into :UTL32K_ACTION
    from V\$PARAMETER
   where NAME = 'max_string_size';
exception
  when NO_DATA_FOUND then
   	:UTL32K_ACTION := '# MAX_STRNG_SIZE NOT SUPPORTED';
  when others then 
    RAISE;
end;
/
spool extendedStringSizeAction.sh
select :UTL32K_ACTION from dual;
spool off
exit
EOF
source extendedStringSizeAction.sh
sqlplus -s sys/oracle@$ORACLE_PDB as sysdba <<-EOF
whenever oserror exit failure
whenever sqlerror exit failure rollback
set heading off pagesize 0 feedback off linesize 400
set trimout on trimspool on termout off echo off sqlprompt ''
VAR INSTALL_ACTION VARCHAR2(1024)
--
begin
  select 'source /opt/oracle/diag/setup/installSampleSchemas.sh' 
    into :INSTALL_ACTION
    from (
	  select count(*) CNT 
	    from ALL_USERS 
	   where USERNAME in ('HR','SH','OE','PM','IX','BI')
	 ) 
   where CNT < 6;
exception
  when NO_DATA_FOUND then
   	:INSTALL_ACTION := '# SAMPLE SCHEMAS ALREADY INSTALLED';
  when others then 
    RAISE;
end;
/
spool sampleSchemas.sh
select :INSTALL_ACTION from dual;
spool off
exit
EOF
source sampleSchemas.sh
mkdir -p /opt/oracle/diag/sql/log
sqlplus system/oracle@$ORACLE_PDB @/opt/oracle/diag/sql/COMPILE_ALL.sql /opt/oracle/diag/sql/log
sqlplus system/oracle@$ORACLE_PDB @/opt/oracle/diag/sql/YADAMU_TEST.sql /opt/oracle/diag/sql/log OFF
