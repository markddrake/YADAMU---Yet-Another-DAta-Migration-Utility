@set CONTAINER_NAME=%1
@set TARGET_IMAGE= %2
@set YADAMU_TEST_NAME=%3
@set CONFIGURATION_FILE=%4
docker stop %CONTAINER_NAME%
docker rm %CONTAINER_NAME%
for /f "tokens=1,2" %%i in ('docker info ^| findstr OSType') do set DOCKER_ENGINE=%%j
if "%DOCKER_ENGINE%" == "linux" ( call :linuxContainer ) else ( call :windowsContainer)
docker logs %CONTAINER_NAME%
goto :eof
:linuxContainer
  @set MAX_MEMORY="16g"
  @set SECURITY_OPTION=seccomp:unconfined
  docker run --name %CONTAINER_NAME% --network YADAMU-NET --memory=%MAX_MEMORY%  --security-opt=%SECURITY_OPTION% -v YADAMU_01-SHARED:/usr/src/YADAMU/mnt -e YADAMU_TEST_NAME=%YADAMU_TEST_NAME% -e TESTNAME=%CONFIGURATION_FILE% -d %TARGET_IMAGE%
  exit /b
:end

:windowsContainer
  @set MAX_MEMORY="4g"
  @set LINUX_HOST=192.168.1.240
  @set LINUX_HOST_LIST=
  REM @set LINUX_HOST_LIST=--add-host="VRTCA09-01:%LINUX_HOST%" --add-host="VRTCA10-01:%LINUX_HOST%" --add-host="VRTCA11-01:%LINUX_HOST%" --add-host="MINIO-01:%LINUX_HOST%" --add-host="AZURE-01:%LINUX_HOST%"
  docker run --name %CONTAINER_NAME% --network Y_OVERLAY %LINUX_HOST_LIST% --memory=%MAX_MEMORY% -v C:\SHARED:C:\YADAMU\mnt -e YADAMU_TEST_NAME=%YADAMU_TEST_NAME% -e TESTNAME=%CONFIGURATION_FILE% -d %TARGET_IMAGE%
  exit /b
:end
