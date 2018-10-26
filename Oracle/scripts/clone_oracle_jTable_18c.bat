@set TNS=ORCL18c
@set MODE=DDL_AND_DATA
@set MDIR=..\JSON\%TNS%\%MODE%
@set DIR=JSON\%TNS%
mkdir %DIR%
sqlplus system/oracle@%TNS% @SQL/COMPILE_ALL
sqlplus system/oracle@%TNS% @TESTS/RECREATE_ORACLE_ALL.sql 3
call scripts\jTable_oracle.bat  %MDIR% %TNS% 3 "" %MODE%
call scripts\export_oracle  %DIR% %TNS% 3 3 %MODE%
sqlplus system/oracle@%TNS% @TESTS/RECREATE_ORACLE_ALL.sql 4
call scripts\jTable_oracle.bat %DIR% %TNS% 4 3 %MODE%
call scripts\export_oracle  %DIR% %TNS% 4 4 %MODE%
dir %DIR%\*3*.json
dir %DIR%\*4*.json
