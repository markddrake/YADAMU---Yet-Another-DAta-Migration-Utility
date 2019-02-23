call env\setEnvironment.bat
@set MODE=DATA_ONLY
@set MDIR=%TESTDATA%\MSSQL
mkdir %MDIR%
call windows\export_MSSQL_All.bat %MDIR% "" ""
