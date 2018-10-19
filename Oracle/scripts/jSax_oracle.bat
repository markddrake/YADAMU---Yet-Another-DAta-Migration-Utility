@set DIR=%~1
@set TNS=%~2
@set UID=%~3
@set VER=%~4
node node\jSaxImport userid=SYSTEM/oracle@%TNS% File=%DIR%\HR%VER%.json toUser=HR%UID%
node node\jSaxImport userid=SYSTEM/oracle@%TNS% File=%DIR%\SH%VER%.json toUser=SH%UID%
node node\jSaxImport userid=SYSTEM/oracle@%TNS% File=%DIR%\OE%VER%.json toUser=OE%UID%
node node\jSaxImport userid=SYSTEM/oracle@%TNS% File=%DIR%\PM%VER%.json toUser=PM%UID%
node node\jSaxImport userid=SYSTEM/oracle@%TNS% File=%DIR%\IX%VER%.json toUser=IX%UID%
node node\jSaxImport userid=SYSTEM/oracle@%TNS% File=%DIR%\BI%VER%.json toUser=BI%UID%