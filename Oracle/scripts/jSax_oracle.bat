@set SRC=%~1
@set TNS=%~2
@set UID=%~3
@set VER=%~4
node node\jSaxImport userid=SYSTEM/oracle@%TNS% File=%SRC%\HR%VER%.json toUser=HR%UID%
node node\jSaxImport userid=SYSTEM/oracle@%TNS% File=%SRC%\SH%VER%.json toUser=SH%UID%
node node\jSaxImport userid=SYSTEM/oracle@%TNS% File=%SRC%\OE%VER%.json toUser=OE%UID%
node node\jSaxImport userid=SYSTEM/oracle@%TNS% File=%SRC%\PM%VER%.json toUser=PM%UID%
node node\jSaxImport userid=SYSTEM/oracle@%TNS% File=%SRC%\IX%VER%.json toUser=IX%UID%
node node\jSaxImport userid=SYSTEM/oracle@%TNS% File=%SRC%\BI%VER%.json toUser=BI%UID%