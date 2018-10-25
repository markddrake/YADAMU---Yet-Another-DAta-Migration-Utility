@set TGT=%~1
@set UID=%~2
@set VER=%~3
node node\export --USERNAME=root --HOSTNAME=192.168.1.250 --PASSWORD=oracle --PORT=3307 --DATABASE=mysql --File=%TGT%\HR%VER%.json --owner=HR%UID%
node node\export --USERNAME=root --HOSTNAME=192.168.1.250 --PASSWORD=oracle --PORT=3307 --DATABASE=mysql --File=%TGT%\SH%VER%.json --owner=SH%UID%
node node\export --USERNAME=root --HOSTNAME=192.168.1.250 --PASSWORD=oracle --PORT=3307 --DATABASE=mysql --File=%TGT%\OE%VER%.json --owner=OE%UID%
node node\export --USERNAME=root --HOSTNAME=192.168.1.250 --PASSWORD=oracle --PORT=3307 --DATABASE=mysql --File=%TGT%\PM%VER%.json --owner=PM%UID%
node node\export --USERNAME=root --HOSTNAME=192.168.1.250 --PASSWORD=oracle --PORT=3307 --DATABASE=mysql --File=%TGT%\IX%VER%.json --owner=IX%UID%
node node\export --USERNAME=root --HOSTNAME=192.168.1.250 --PASSWORD=oracle --PORT=3307 --DATABASE=mysql --File=%TGT%\BI%VER%.json --owner=BI%UID%
