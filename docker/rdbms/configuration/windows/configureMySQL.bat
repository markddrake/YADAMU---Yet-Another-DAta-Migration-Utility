@set CONTAINER_NAME=%1
@set MYSQL_VERSION=%2
docker exec -it                         %CONTAINER_NAME% powershell -command New-Item -Force -ItemType directory -Path c:\stage\sql
REM 
REM Cannot copy to Windows Container when it is running. 
REM Cannot copy to Volume mapped folder when container is not running.
REM
docker stop                             %CONTAINER_NAME%
docker cp docker/rdbms/mysql/setup      %CONTAINER_NAME%:c:\stage
docker cp docker/rdbms/mysql/testdata   %CONTAINER_NAME%:c:\stage
docker cp src/sql/mysql/.               %CONTAINER_NAME%:c:\stage\sql
docker cp src/install/mysql/js          %CONTAINER_NAME%:c:\stage
docker start                            %CONTAINER_NAME%
docker exec -it                         %CONTAINER_NAME% robocopy c:\stage c:\ProgramData\MySQL\%MYSQL_VERSION%\stage /MIR 
docker exec -it                         %CONTAINER_NAME% cmd /c c:\ProgramData\MySQL\%MYSQL_VERSION%\Stage\setup\configure.bat