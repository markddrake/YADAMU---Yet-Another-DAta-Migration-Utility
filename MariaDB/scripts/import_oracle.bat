@set SRC=%~1
@set UID=%~2
@set VER=%~3
node node\jSaxImport --USERNAME=root --HOSTNAME=192.168.1.250 --PASSWORD=oracle  --PORT=3307 --DATABASE=mysql --File=%SRC%\HR%VER%.json --toUser=HR%UID%
node node\jSaxImport --USERNAME=root --HOSTNAME=192.168.1.250 --PASSWORD=oracle  --PORT=3307 --DATABASE=mysql --File=%SRC%\SH%VER%.json --toUser=SH%UID%
node node\jSaxImport --USERNAME=root --HOSTNAME=192.168.1.250 --PASSWORD=oracle  --PORT=3307 --DATABASE=mysql --File=%SRC%\OE%VER%.json --toUser=OE%UID%
node node\jSaxImport --USERNAME=root --HOSTNAME=192.168.1.250 --PASSWORD=oracle  --PORT=3307 --DATABASE=mysql --File=%SRC%\PM%VER%.json --toUser=PM%UID%
node node\jSaxImport --USERNAME=root --HOSTNAME=192.168.1.250 --PASSWORD=oracle  --PORT=3307 --DATABASE=mysql --File=%SRC%\IX%VER%.json --toUser=IX%UID%
node node\jSaxImport --USERNAME=root --HOSTNAME=192.168.1.250 --PASSWORD=oracle  --PORT=3307 --DATABASE=mysql --File=%SRC%\BI%VER%.json --toUser=BI%UID%

