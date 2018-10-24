@set TNS=ORCL18c
@set MODE=DDL_AND_DATA
mkdir JSON\%TNS%
sqlplus system/oracle@%TNS% @SQL/COMPILE_ALL
sqlplus system/oracle@%TNS% @TESTS/RECREATE_ORACLE_ALL.sql 3
call scripts\jSax_oracle.bat  ..\JSON\%TNS%\%MODE% %TNS% 3 "" %MODE%
call scripts\export_oracle  JSON\%TNS% %TNS% 3 3 %MODE%
sqlplus system/oracle@%TNS% @TESTS/RECREATE_ORACLE_ALL.sql 4
call scripts\jSax_oracle.bat JSON\%TNS% %TNS% 4 3 %MODE%
call scripts\export_oracle  JSON\%TNS% %TNS% 4 4 %MODE%
dir JSON\%TNS%\*3*.json
dir JSON\%TNS%\*4*.json
