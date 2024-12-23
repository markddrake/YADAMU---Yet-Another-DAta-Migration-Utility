set docker_context=%1
docker compose --file docker\dockerfiles\linux\IBMDB2-01.yml up -d
echo 'Waiting for IBM DB2 Startup'
timeout /nobreak /t 600
start "IBM DB2 Configuration" /MIN /WAIT /D C:\DEVELOPMENT\YADAMU CMD /c docker\rdbms\configuration\linux\configureDB2 IBMDB2-01
start "IBM DB2 Test Suite" /MIN /WAIT /D C:\DEVELOPMENT\YADAMU CMD /c docker\bin\runTestSuite db2 > db2.log
echo 'Waiting IBM DB2 Test Suite to complete.'
docker wait YADAMU-01
docker compose --file docker\dockerfiles\linux\IBMDB2-01.yml down -v
docker compose --file docker\dockerfiles\linux\COCKROACH-01.yml up -d
echo 'Waiting for Cockroach Startup'
timeout /nobreak /t 60
start "Cockroach Configuration" /MIN /WAIT /D C:\DEVELOPMENT\YADAMU CMD /c docker\rdbms\configuration\linux\configureCockroach ROACH01-01
start "Cockroach Test Suite" /MIN /WAIT /D C:\DEVELOPMENT\YADAMU CMD /c docker\bin\runTestSuite cdb > cdb.log
echo 'Waiting for Cockroach Test Suite to complete.'
docker wait YADAMU-01
docker compose --file docker\dockerfiles\linux\COCKROACH-01.yml down -v
docker compose --file docker\dockerfiles\linux\YUGABYTE-01.yml up -d
echo 'Waiting for Yugabyte Startup'
timeout /nobreak /t 60
start "Yugabyte Configuration" /MIN /WAIT /D C:\DEVELOPMENT\YADAMU CMD /c docker\rdbms\configuration\linux\configureYugabyte YUGABYTE-01
start "Yugabyte Test Suite" /MIN /WAIT /D C:\DEVELOPMENT\YADAMU CMD /c docker\bin\runTestSuite ydb > ydb.log
echo 'Waiting for Yugabyte Test Suite to complete.'
docker wait YADAMU-01
docker compose --file docker\dockerfiles\linux\YUGABYTE-01.yml down -v

