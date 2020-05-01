docker stop YADAMU-01
docker rm YADAMU-01
docker volume rm YADAMU_01_MNT
docker volume create YADAMU_01_MNT
docker rmi yadamu/regression:latest
set DOCKER_BUILDKIT=1
docker build -t  yadamu/regression:latest . -f dockerfiles/regression/Dockerfile
docker run --name YADAMU-01 -v YADAMU_01_MNT:/usr/src/YADAMU/mnt --network YADAMU-NET -d yadamu/regression:latest 
docker logs YADAMU-01
docker cp YADAMU-01:/usr/src/YADAMU/mnt/log .

