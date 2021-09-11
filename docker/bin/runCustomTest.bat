@set TESTNAME=%1
docker stop YADAMU-01
docker rm YADAMU-01
docker run --security-opt=seccomp:unconfined --name YADAMU-01 --memory="16g" -v YADAMU_01-SHARED:/usr/src/YADAMU/mnt --network YADAMU-NET -d -e YADAMU_TEST_NAME=custom -e TESTNAME=%TESTNAME% yadamu/regression:latest
docker logs YADAMU-01