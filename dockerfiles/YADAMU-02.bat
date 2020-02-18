docker stop YADAMU-01
docker rm YADAMU-01
docker volume rm YADAMU_01_MNT
docker volume create YADAMU_01_MNT
docker rmi yadamu:latest
set DOCKER_BUILDKIT=1
docker build -t yadamu . -f dockerfiles\yadamu.regress
docker run --name YADAMU-01 -v YADAMU_01_MNT:/usr/src/YADAMU/mnt  --network YADAMU-NET-d yadamu:latest