@set CONTAINER_NAME=%1
docker exec -it                                 %CONTAINER_NAME% powershell -command New-Item -Force -ItemType directory -Path c:\stage\sql
REM 
REM Cannot copy to Windows Container when it is running. 
REM Cannot copy to Volume mapped folder when container is not running.
REM
docker stop                                     %CONTAINER_NAME%
docker cp docker/rdbms/mariadb/setup/utf-8.cnf  %CONTAINER_NAME%:c:\stage
docker cp docker/rdbms/mariadb/setup            %CONTAINER_NAME%:c:\stage
docker cp docker/rdbms/mariadb/testdata         %CONTAINER_NAME%:c:\stage
docker cp src/install/mariadb/js                %CONTAINER_NAME%:c:\stage
docker start                                    %CONTAINER_NAME%
docker exec -it                                 %CONTAINER_NAME% robocopy c:\stage c:\ProgramData\MariaDB\10\stage /MIR 
docker exec -it                                 %CONTAINER_NAME% cmd /c c:\ProgramData\MariaDB\10\stage\setup\configure.bat