docker rmi -f yadamu/base:latest
set DOCKER_BUILDKIT=1
docker build -t  yadamu/base:latest . -f docker/dockerfiles/yadamu
