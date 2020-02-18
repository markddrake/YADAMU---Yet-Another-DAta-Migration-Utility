SET PATH=%PATH%;"c:\Program Files\Docker Toolbox";"c:\Program Files\Oracle\VirtualBox"
set DCONFIG=%CD%
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
docker-machine rm -y default
rmdir /s /q machine machine\machines\default
docker-machine create -d virtualbox --virtualbox-disk-size "65536" --virtualbox-memory %VBOX_PHYSICAL_MEMORY% --virtualbox-cpu-count %VBOX_PROCESSOR_COUNT% default
if not exist volumes mkdir volumes
cd volumes
REM Create and Attach Disks. 
REM Generate scripts to create and mount Unix File Systems
set /A SATA_PORT = 2
echo #/bin/bash> %DCONFIG%\sh\createFileSystems.sh
echo:>%DCONFIG%\sh\fstabEntries
SETLOCAL ENABLEDELAYEDEXPANSION 
for /f "usebackq tokens=1-2 delims=," %%a in ("%DCONFIG%\bin\volumes.csv") do (
  if exist %%a.vdi del %%a.vdi
  VBoxManage createhd --filename %%a.vdi --size %%b
  VboxManage storageattach default --storagectl SATA --port !SATA_PORT! --medium  %%a.vdi --type hdd 
  set /A UNIX_ID_CODE = 96 + !SATA_PORT!
  cmd /c exit !UNIX_ID_CODE!
  set "LINUX_DEVICE_ID=!=ExitCodeAscii!"
  echo sfdisk /dev/sd!LINUX_DEVICE_ID! ^<^< EOF>> %DCONFIG%\sh\createFileSystems.sh
  echo , %%bM>> %DCONFIG%\sh\createFileSystems.sh
  echo EOF>> %DCONFIG%\sh\createFileSystems.sh
  echo mkfs --type ext4 /dev/sd!LINUX_DEVICE_ID!1>> %DCONFIG%\sh\createFileSystems.sh
  echo e2label /dev/sd!LINUX_DEVICE_ID!1 %%a>> %DCONFIG%\sh\createFileSystems.sh
  echo mkdir /mnt/sda1/var/lib/docker/volumes/%%a>> %DCONFIG%\sh\createFileSystems.sh
  echo LABEL=%%a /mnt/sda1/var/lib/docker/volumes/%%a ext4 defaults 0 0 >>%DCONFIG%\sh\fstabEntries
  set /A SATA_PORT = !SATA_PORT! + 1
)
ENDLOCAL
cd %DCONFIG%
powershell.exe -noninteractive -NoProfile -ExecutionPolicy Bypass -Command "& {[IO.File]::WriteAllText('%DCONFIG%\sh\createFileSystems.sh', ([IO.File]::ReadAllText('%DCONFIG%\sh\createFileSystems.sh') -replace \"`r`n\", \"`n\"))};"
powershell.exe -noninteractive -NoProfile -ExecutionPolicy Bypass -Command "& {[IO.File]::WriteAllText('%DCONFIG%\sh\fstabEntries', ([IO.File]::ReadAllText('%DCONFIG%\sh\fstabEntries') -replace \"`r`n\", \"`n\"))};"cd %DCONFIG%
docker-machine env    
FOR /f "tokens=*" %%i IN ('docker-machine env') DO @%%i                                                          
for /f "tokens=2 delims=:/" %%a in ("%DOCKER_HOST%") DO (SET DOCKER_IP_ADDR=%%a)
scp %DCONFIG%\sh\createFileSystems.sh docker@%DOCKER_IP_ADDR%:
ssh docker@%DOCKER_IP_ADDR% "sudo /bin/bash createFileSystems.sh" 
scp %DCONFIG%\sh\fstabEntries docker@%DOCKER_IP_ADDR%:/var/lib/boot2docker
scp %DCONFIG%\sh\bootlocal.sh docker@%DOCKER_IP_ADDR%:/var/lib/boot2docker
ssh docker@%DOCKER_IP_ADDR% "sudo /bin/bash /var/lib/boot2docker/bootlocal.sh"


