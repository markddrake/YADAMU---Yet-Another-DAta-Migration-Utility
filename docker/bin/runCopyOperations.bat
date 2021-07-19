docker rm YADAMU-01
docker run --name YADAMU-01 --memory="16g" -v YADAMU_01_MNT:/usr/src/YADAMU/mnt --network YADAMU-NET -d -e YADAMU_TEST_NAME=copy yadamu/secure:latest
docker logs YADAMU-01

