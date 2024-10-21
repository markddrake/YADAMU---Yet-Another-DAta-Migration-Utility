set docker_context=%1
start "Command Line Test Suite" /WAIT /D C:\DEVELOPMENT\YADAMU CMD /c docker\bin\runCommandLineTests.bat > commandLineTests.log
echo 'Waiting for Command Line Test Suite to complete.'
docker wait YADAMU-01
start "Oracle 11g Test Suite" /MIN /WAIT /D C:\DEVELOPMENT\YADAMU CMD /c docker\bin\runTestSuite oracle11g > oracle11g.log
echo 'Waiting for Oracle 11g Test Suite to complete.'
docker wait YADAMU-01
start "Oracle 19c Test Suite" /MIN /WAIT /D C:\DEVELOPMENT\YADAMU CMD /c docker\bin\runTestSuite oracle19c > oracle19c.log
echo 'Waiting for Oracle 19c Test Suite to complete.'
docker wait YADAMU-01
docker compose --file docker\dockerfiles\linux\ORA2103-01.yml up -d
echo 'Waiting for Oracle 21c Startup'
timeout /nobreak /t 600
start "Oracle 21c Configuration" /MIN /WAIT /D C:\DEVELOPMENT\YADAMU CMD /c docker\rdbms\configuration\linux\configureOracle ORA2103-01
start "Oracle 21c Test Suite" /MIN /WAIT /D C:\DEVELOPMENT\YADAMU CMD /c docker\bin\runTestSuite oracle21c > oracle21c.log
echo 'Waiting for Oracle 21c Test Suite to complete.'
docker wait YADAMU-01
docker compose --file docker\dockerfiles\linux\ORA2103-01.yml down -v
docker compose --file docker\dockerfiles\linux\ORA2305-01.yml up -d
echo 'Waiting for Oracle 23ai Startup'
timeout /nobreak /t 600
start "Oracle 23ai Configuration" /MIN /WAIT /D C:\DEVELOPMENT\YADAMU CMD /c docker\rdbms\configuration\linux\configureOracle ORA2305-01
start "Oracle 23ai Test Suite" /MIN /WAIT /D C:\DEVELOPMENT\YADAMU CMD /c docker\bin\runTestSuite oracle23ai > oracle23ai.log
echo 'Waiting for Oracle 23ai Test Suite to complete.'
docker wait YADAMU-01
docker compose --file docker\dockerfiles\linux\ORA2305-01.yml down -v
docker compose --file docker\dockerfiles\linux\VRTCA09-01.yml up -d
echo 'Waiting for Vertica 09 Startup'
timeout /nobreak /t 300
"c:\Program Files\Vertica Systems\VSQL64\vsql.exe" -Udbadmin -ddocker -hyadamu-db1 -p54331 -f  src\sql\vertica\YADAMU_IMPORT.sql
"c:\Program Files\Vertica Systems\VSQL64\vsql.exe" -Udbadmin -ddocker -hyadamu-db1 -p54331 -f  src\sql\vertica\YADAMU_COMPARE.sql
start "Vertica 9 Test Suite" /MIN /WAIT /D C:\DEVELOPMENT\YADAMU CMD /c docker\bin\runTestSuite vertica09 > vetica09.log
echo 'Waiting for Vertica 09 Test Suite to complete.'
docker wait YADAMU-01
docker compose --file docker\dockerfiles\linux\VRTCA09-01.yml down -v
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
start "Yugabyte Configuration" /MIN /WAIT /D C:\DEVELOPMENT\YADAMU CMD /c docker\rdbms\configuration\linux\configureYugabyte yb-tserver-n1
start "Yugabyte Test Suite" /MIN /WAIT /D C:\DEVELOPMENT\YADAMU CMD /c docker\bin\runTestSuite ydb > ydb.log
echo 'Waiting for Yugabyte Test Suite to complete.'
docker wait YADAMU-01
docker compose --file docker\dockerfiles\linux\YUGABYTE-01.yml down -v

