@set SRC=%~1
@set UID=%~2
@set VER=%~3
node node\jTableImport --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle --File=%SRC%\HR%VER%.json --toUser=HR%UID%
node node\jTableImport --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle --File=%SRC%\SH%VER%.json --toUser=SH%UID%
node node\jTableImport --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle --File=%SRC%\OE%VER%.json --toUser=OE%UID%
node node\jTableImport --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle --File=%SRC%\PM%VER%.json --toUser=PM%UID%
node node\jTableImport --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle --File=%SRC%\IX%VER%.json --toUser=IX%UID%
node node\jTableImport --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle --File=%SRC%\BI%VER%.json --toUser=BI%UID%

