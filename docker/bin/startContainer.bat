docker rm YADAMU-01
docker run --name YADAMU-01 --memory="16g" -v YADAMU_01_MNT:/usr/src/YADAMU/mnt --network YADAMU-NET -d -e YADAMU_TEST_NAME=interactive yadamu/regression:latest
docker exec -it YADAMU-01 /bin/bash