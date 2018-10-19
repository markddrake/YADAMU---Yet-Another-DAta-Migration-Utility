set TNS=ORCL18c
mkdir JSON\%TNS%
sqlplus system/oracle@%TNS% @SQL/COMPILE_ALL
sqlplus system/oracle@%TNS% @TESTS/RECREATE_ORACLE_ALL.sql 3
call scripts\jTable_oracle.bat  ..\JSON\%TNS%\DATA_ONLY %TNS% 3 "" DATA_ONLY
call scripts\export_oracle  JSON\%TNS% %TNS% 3 3 DATA_ONLY
sqlplus system/oracle@%TNS% @TESTS/RECREATE_ORACLE_ALL.sql 4
call scripts\jTable_oracle.bat JSON\%TNS% %TNS% 4 3 DATA_ONLY
call scripts\export_oracle  JSON\%TNS% %TNS% 4 4 DATA_ONLY
dir JSON\%TNS%\*3*.json
dir JSON\%TNS%\*4*.json
