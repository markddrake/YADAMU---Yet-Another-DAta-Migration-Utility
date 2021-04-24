@set TESTNAME=%1
docker stop YADAMU-01
docker rm YADAMU-01
docker run --name YADAMU-01 --memory="16g" -v YADAMU_01_MNT:/usr/src/YADAMU/mnt --network YADAMU-NET -d -e YADAMU_TEST_NAME=local -e TESTNAME=%TESTNAME% yadamu/regression:latest
docker logs YADAMU-01

