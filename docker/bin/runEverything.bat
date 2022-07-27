if defined %1 set  MSSQL14=%1
if not defined MSSQL14 set /p MSSQL14="SQL Server 2014 IP Address :"
docker rm YADAMU-01
docker run --security-opt=seccomp:unconfined --name YADAMU-01 --memory="16g" -v YADAMU_01-SHARED:/usr/src/YADAMU/mnt --network YADAMU-NET -d -e YADAMU_TEST_NAME=everything -e TESTNAME=mssql2014TestSuite --add-host="MSSQL14-01:%MSSQL14%" yadamu/secure:latest
docker logs YADAMU-01
