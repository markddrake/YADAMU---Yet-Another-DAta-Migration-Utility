@set SRC=%~1
@set SCHEMAVER=%~2
@set FILEVER=%~3
node %YADAMU_DB_ROOT%\node\import --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=HR%SCHEMAVER% file=%SRC%\HR%FILEVER%.json to_user=\"dbo\" log_file=%IMPORTLOG% mode=%MODE%
node %YADAMU_DB_ROOT%\node\import --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=SH%SCHEMAVER% file=%SRC%\SH%FILEVER%.json to_user=\"dbo\" log_file=%IMPORTLOG% mode=%MODE%
node %YADAMU_DB_ROOT%\node\import --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=OE%SCHEMAVER% file=%SRC%\OE%FILEVER%.json to_user=\"dbo\" log_file=%IMPORTLOG% mode=%MODE%
node %YADAMU_DB_ROOT%\node\import --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=PM%SCHEMAVER% file=%SRC%\PM%FILEVER%.json to_user=\"dbo\" log_file=%IMPORTLOG% mode=%MODE%
node %YADAMU_DB_ROOT%\node\import --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=IX%SCHEMAVER% file=%SRC%\IX%FILEVER%.json to_user=\"dbo\" log_file=%IMPORTLOG% mode=%MODE%
node %YADAMU_DB_ROOT%\node\import --username=%DB_USER% --hostname=%DB_HOST% --password=%DB_PWD% --database=BI%SCHEMAVER% file=%SRC%\BI%FILEVER%.json to_user=\"dbo\" log_file=%IMPORTLOG% mode=%MODE%

