@set CONTAINER_NAME=YADAMU-01
docker stop %CONTAINER_NAME%
docker rm %CONTAINER_NAME%
for /f "tokens=1,2" %%i in ('docker info ^| findstr OSType') do set DOCKER_ENGINE=%%j
if "%DOCKER_ENGINE%" == "linux" ( call :linuxContainer ) else ( call :windowsContainer)
goto :eof
:linuxContainer
  if defined %1 set  MSSQL14=%1
  if not defined MSSQL14 set /p MSSQL14="SQL Server 2014 IP Address :"
  docker rm %CONTAINER_NAME%
  docker run --security-opt=seccomp:unconfined --name %CONTAINER_NAME% --memory="16g" -v YADAMU_01-SHARED:/usr/src/YADAMU/mnt --network YADAMU-NET -e YADAMU_TEST_NAME=everything --add-host="MSSQL14-01:%MSSQL14%" -d yadamu/secure:latest
  docker logs %CONTAINER_NAME%
  exit /b
:end

:windowsContainerSwarm
  @set MAX_MEMORY="4g"
  @set DISC_SIZE="100GB"
  docker run --name %CONTAINER_NAME% --network YADAMU-NET --storage-opt "size=%DISC_SIZE%"  --memory %MAX_MEMORY% -v C:\SHARED:C:\YADAMU\mnt -e YADAMU_TEST_NAME=everything -d yadamu/secure:latest
  docker logs %CONTAINER_NAME%
  exit /b
:end

:windowsContainer
  @set MAX_MEMORY="4g"
  @set DISC_SIZE="100GB"
  @set HOSTS=--add-host="VRTCA23-01:192.168.1.236" --add-host="VRTCA10-01:192.168.1.236" --add-host="azure-01:192.168.1.236" --add-host="minio-01:192.168.1.236"
  docker run --name %CONTAINER_NAME% --network YADAMU-NET --storage-opt "size=%DISC_SIZE%"  --memory %MAX_MEMORY% -v C:\SHARED:C:\YADAMU\mnt %HOSTS% -e YADAMU_TEST_NAME=everything -d yadamu/secure:latest
  docker logs %CONTAINER_NAME%
  exit /b
:end