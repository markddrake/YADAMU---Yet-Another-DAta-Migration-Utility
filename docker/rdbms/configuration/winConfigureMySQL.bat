@set CONTAINER_NAME=%1
docker exec -it                         %CONTAINER_NAME% powershell -command New-Item -Force -ItemType directory -Path c:\stage\sql
REM 
REM Cannot copy to Windows Container when it is running. 
REM Cannot copy to Volume mapped folder when container is not running.
REM
docker stop                             %CONTAINER_NAME%
docker cp docker/rdbms/mysql/setup      %CONTAINER_NAME%:c:\stage
docker cp docker/rdbms/mysql/testdata   %CONTAINER_NAME%:c:\stage
docker cp src/install/mysql/sql         %CONTAINER_NAME%:c:\stage
docker cp qa/sql/mysql/YADAMU_TEST.sql  %CONTAINER_NAME%:c:\stage\sql
docker start                            %CONTAINER_NAME%
docker exec -it                         %CONTAINER_NAME% robocopy c:\stage c:\ProgramData\MySQL\8.0\stage /MIR 
docker exec -it                         %CONTAINER_NAME% cmd /c c:\ProgramData\MySQL\8.0\Stage\setup\configure.bat