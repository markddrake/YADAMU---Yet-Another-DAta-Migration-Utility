docker rmi -f yadamu/service:latest
set DOCKER_BUILDKIT=1
docker build -t yadamu/service . -f docker/dockerfiles/linux/service
