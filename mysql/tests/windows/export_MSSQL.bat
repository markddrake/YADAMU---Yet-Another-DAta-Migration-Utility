@SET TGT=%~1
@SET VER=%~2
@SET SCHVER=%~3
node ..\node\export   --USERNAME=%DB_USER% --HOSTNAME=%DB_HOST% --PASSWORD=%DB_PWD%  --PORT=%DB_PORT% --DATABASE=%DB_DBNAME% owner=\"Northwind%SCHVER%\"       file=%TGT%\Northwind%VER%.json        mode=%MODE% logFile=%EXPORTLOG%
node ..\node\export   --USERNAME=%DB_USER% --HOSTNAME=%DB_HOST% --PASSWORD=%DB_PWD%  --PORT=%DB_PORT% --DATABASE=%DB_DBNAME% owner=\"Sales%SCHVER%\"           file=%TGT%\Sales%VER%.json            mode=%MODE% logFile=%EXPORTLOG%
node ..\node\export   --USERNAME=%DB_USER% --HOSTNAME=%DB_HOST% --PASSWORD=%DB_PWD%  --PORT=%DB_PORT% --DATABASE=%DB_DBNAME% owner=\"Person%SCHVER%\"          file=%TGT%\Person%VER%.json           mode=%MODE% logFile=%EXPORTLOG%
node ..\node\export   --USERNAME=%DB_USER% --HOSTNAME=%DB_HOST% --PASSWORD=%DB_PWD%  --PORT=%DB_PORT% --DATABASE=%DB_DBNAME% owner=\"Production%SCHVER%\"      file=%TGT%\Production%VER%.json       mode=%MODE% logFile=%EXPORTLOG%
node ..\node\export   --USERNAME=%DB_USER% --HOSTNAME=%DB_HOST% --PASSWORD=%DB_PWD%  --PORT=%DB_PORT% --DATABASE=%DB_DBNAME% owner=\"Purchasing%SCHVER%\"      file=%TGT%\Purchasing%VER%.json       mode=%MODE% logFile=%EXPORTLOG%
node ..\node\export   --USERNAME=%DB_USER% --HOSTNAME=%DB_HOST% --PASSWORD=%DB_PWD%  --PORT=%DB_PORT% --DATABASE=%DB_DBNAME% owner=\"HumanResources%SCHVER%\"  file=%TGT%\HumanResources%VER%.json   mode=%MODE% logFile=%EXPORTLOG%
node ..\node\export   --USERNAME=%DB_USER% --HOSTNAME=%DB_HOST% --PASSWORD=%DB_PWD%  --PORT=%DB_PORT% --DATABASE=%DB_DBNAME% owner=\"DW%SCHVER%\"              file=%TGT%\AdventureWorksDW%VER%.json mode=%MODE% logFile=%EXPORTLOG%
