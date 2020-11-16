docker rm YADAMU-01
docker run --name YADAMU-01 --memory="16g" -v YADAMU_01_MNT:/usr/src/YADAMU/mnt --network YADAMU-NET -d -e YADAMU_TEST_NAME=export yadamu/regression:latest
docker logs YADAMU-01
docker cp YADAMU-01:/usr/src/YADAMU/mnt/log .
