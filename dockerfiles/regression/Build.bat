docker stop YADAMU-01
docker rm YADAMU-01
docker volume rm YADAMU_01_MNT
docker volume create YADAMU_01_MNT
docker rmi -f yadamu/regression:latest
set DOCKER_BUILDKIT=1
docker build -t  yadamu/regression:latest . -f dockerfiles/regression/Dockerfile
