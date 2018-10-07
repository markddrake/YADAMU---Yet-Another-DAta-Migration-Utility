mkdir JSON\ORCL18c
sqlplus system/oracle@ORCL18c @SQL/COMPILE_ALL
sqlplus system/oracle@ORCL18c @TESTS/RECREATE_ORACLE_ALL.sql 1
call scripts\import_master_all.bat ORCL18C 18C DATA_ONLY  1
call scripts\export_oracle_all ORCL18c 1 1 DATA_ONLY
sqlplus system/oracle@ORCL18c @TESTS/RECREATE_ORACLE_ALL.sql 2
call scripts\import_oracle_all ORCL18c 1 2
call scripts\export_oracle_all ORCL18c 2 2 DATA_ONLY
dir JSON\ORCL18c\*1*.json
dir JSON\ORCL18c\*2*.json
