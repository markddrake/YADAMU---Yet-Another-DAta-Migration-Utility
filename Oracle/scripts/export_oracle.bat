@set SRC=%~1
@set TNS=%~2
@set UID=%~3
@set VER=%~4
@set MODE=%~5
node node\export userid=SYSTEM/oracle@%TNS% File=%SRC%\HR%VER%.json owner=HR%UID% MODE=%MODE%
node node\export userid=SYSTEM/oracle@%TNS% File=%SRC%\SH%VER%.json owner=SH%UID% MODE=%MODE%
node node\export userid=SYSTEM/oracle@%TNS% File=%SRC%\OE%VER%.json owner=OE%UID% MODE=%MODE%
node node\export userid=SYSTEM/oracle@%TNS% File=%SRC%\PM%VER%.json owner=PM%UID% MODE=%MODE%
node node\export userid=SYSTEM/oracle@%TNS% File=%SRC%\IX%VER%.json owner=IX%UID% MODE=%MODE%
node node\export userid=SYSTEM/oracle@%TNS% File=%SRC%\BI%VER%.json owner=BI%UID% MODE=%MODE% 