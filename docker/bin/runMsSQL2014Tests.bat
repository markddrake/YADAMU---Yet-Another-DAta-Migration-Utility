docker rm YADAMU-01
docker run --security-opt=seccomp:unconfined --name YADAMU-01 --memory="16g" -v YADAMU_01-SHARED:/usr/src/YADAMU/mnt --network YADAMU-NET -d -e YADAMU_TEST_NAME=mssql2014TestSuite --add-host="MSSQL14-01:%MSSQL14%" yadamu/regression:latest
docker logs YADAMU-01
