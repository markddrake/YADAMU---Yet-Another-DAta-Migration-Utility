call env\setEnvironment.bat
sqlplus %DB_USER%/%DB_PWD%@%DB_CONNECTION% @..\sql\COMPILE_ALL.sql %LOGDIR%
@set MODE=DDL_ONLY
@set MDIR=%TESTDATA%\%ORCL%\%MODE%
mkdir %MDIR%
call windows\export_Oracle %MDIR% "" "" %MODE% logFile=%EXPORTLOG%
@set MODE=DATA_ONLY
@set MDIR=%TESTDATA%\%ORCL%\%MODE%
mkdir %MDIR%
call windows\export_Oracle  %MDIR% "" "" %MODE% logFile=%EXPORTLOG%
@set MODE=DDL_AND_DATA
@set MDIR=%TESTDATA%\%ORCL%\%MODE%
mkdir %MDIR%
call windows\export_Oracle  %MDIR% "" "" %MODE% logFile=%EXPORTLOG%
