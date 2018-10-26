@set SRC=%~1
@set UID=%~2
@set VER=%~3
node node\export --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle --File=%SRC%\HR%VER%.json --owner=HR%UID%
node node\export --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle --File=%SRC%\SH%VER%.json --owner=SH%UID%
node node\export --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle --File=%SRC%\OE%VER%.json --owner=OE%UID%
node node\export --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle --File=%SRC%\PM%VER%.json --owner=PM%UID%
node node\export --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle --File=%SRC%\IX%VER%.json --owner=IX%UID%
node node\export --USERNAME=postgres --HOSTNAME=192.168.1.250 --PASSWORD=oracle --File=%SRC%\BI%VER%.json --owner=BI%UID%

