@set TESTPATH=%1
@set FILENAME=%~n1
docker stop YADAMU-02
docker rm  YADAMU-02
docker create --name YADAMU-02 --memory="16g" -v YADAMU_01_MNT:/usr/src/YADAMU/mnt --network YADAMU-NET -e YADAMU_TEST_NAME=local -e TESTNAME=%FILENAME% yadamu/regression:latest 
docker cp %TESTPATH% YADAMU-02:/usr/src/YADAMU/mnt/local
docker start YADAMU-02 
docker logs -f YADAMU-02