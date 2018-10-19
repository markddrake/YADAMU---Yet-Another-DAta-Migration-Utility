echo off
set DIR=%~1
set TNS=%~2
set UID=%~3
set VER=%~4
set MODE=%~5
echo on
node node\export userid=SYSTEM/oracle@%TNS% File=%DIR%\HR%VER%.json owner=HR%UID% MODE=%MODE%
node node\export userid=SYSTEM/oracle@%TNS% File=%DIR%\SH%VER%.json owner=SH%UID% MODE=%MODE%
node node\export userid=SYSTEM/oracle@%TNS% File=%DIR%\OE%VER%.json owner=OE%UID% MODE=%MODE%
node node\export userid=SYSTEM/oracle@%TNS% File=%DIR%\PM%VER%.json owner=PM%UID% MODE=%MODE%
node node\export userid=SYSTEM/oracle@%TNS% File=%DIR%\IX%VER%.json owner=IX%UID% MODE=%MODE%
node node\export userid=SYSTEM/oracle@%TNS% File=%DIR%\BI%VER%.json owner=BI%UID% MODE=%MODE% 