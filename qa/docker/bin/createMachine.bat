SET PATH=%PATH%;"c:\Program Files\Docker Toolbox";"c:\Program Files\Oracle\VirtualBox"
cd %USERPROFILE%\.docker
for /f "tokens=2 delims==" %%a in (
      'wmic computersystem get totalphysicalmemory /value'
    ) do for /f "delims=" %%b in (
      "%%~a"
    ) do set "TOTAL_PHYSICAL_MEMORY=%%~b"
echo %TOTAL_PHYSICAL_MEMORY%
REM Assume we can use 75% of the Physical Memory for VBOX
REM set /A VBOX_PHYSICAL_MEMORY = (%TOTAL_PHYSICAL_MEMORY% * 3) /4
For /F "Tokens=1" %%I in ('powershell -command "[Math]::Floor([decimal]((%TOTAL_PHYSICAL_MEMORY%/4)*3)/1024/1024)"') Do Set VBOX_PHYSICAL_MEMORY=%%I
set /A VBOX_PROCESSOR_COUNT = %NUMBER_OF_PROCESSORS% / 2

docker-machine rm default
rmdir /s /q machine machine\machines\default
docker-machine create -d virtualbox --virtualbox-disk-size "65536" --virtualbox-memory %VBOX_PHYSICAL_MEMORY% --virtualbox-cpu-count %VBOX_PROCESSOR_COUNT% default
if not exist volumes mkdir volumes
cd volumes

if exist ORA1903-01.vdi del ORA1903-01.vdi
if exist ORA1803-01.vdi del ORA1803-01.vdi
if exist ORA1220-01.vdi del ORA1220-01.vdi
if exist ORA1220-01.vdi del ORA1210-01.vdi
if exist ORA1120-01.vdi del ORA1120-01.vdi
if exist MYSQL80-01.vdi del MYSQL80-01.vdi
if exist MSSQL17-01.vdi del MSSQL17-01.vdi
if exist MSSQL19-01.vdi del MSSQL19-01.vdi
if exist PGSQL12-01.vdi del PGSQL12-01.vdi
if exist MARIA10-01.vdi del MARIA10-01.vdi

VBoxManage createhd --filename ORA1903-01.vdi --size 16384
VBoxManage createhd --filename ORA1803-01.vdi --size 16384
VBoxManage createhd --filename ORA1220-01.vdi --size 16384
VBoxManage createhd --filename ORA1210-01.vdi --size 16384
VBoxManage createhd --filename ORA1120-01.vdi --size 16384
VBoxManage createhd --filename MYSQL80-01.vdi --size 16384
VBoxManage createhd --filename MSSQL17-01.vdi --size 16384
VBoxManage createhd --filename MSSQL19-01.vdi --size 16384
VBoxManage createhd --filename PGSQL12-01.vdi --size 16384
VBoxManage createhd --filename MARIA10-01.vdi --size 16384

VboxManage storageattach default --storagectl SATA --port  2 --medium  ORA1903-01.vdi --type hdd 
VboxManage storageattach default --storagectl SATA --port  3 --medium  ORA1803-01.vdi --type hdd
VboxManage storageattach default --storagectl SATA --port  4 --medium  ORA1220-01.vdi --type hdd
VboxManage storageattach default --storagectl SATA --port  5 --medium  ORA1210-01.vdi --type hdd
VboxManage storageattach default --storagectl SATA --port  6 --medium  ORA1120-01.vdi --type hdd
VboxManage storageattach default --storagectl SATA --port  7 --medium  MYSQL80-01.vdi --type hdd
VboxManage storageattach default --storagectl SATA --port  8 --medium  MSSQL17-01.vdi --type hdd
VboxManage storageattach default --storagectl SATA --port  9 --medium  MSSQL19-01.vdi --type hdd
VboxManage storageattach default --storagectl SATA --port 10 --medium  PGSQL12-01.vdi --type hdd
VboxManage storageattach default --storagectl SATA --port 11 --medium  MARIA10-01.vdi --type hdd
                                                                       