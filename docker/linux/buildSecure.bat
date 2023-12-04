docker rmi -f yadamu/secure:latest
set DOCKER_BUILDKIT=1
docker build -t  yadamu/secure:latest . -f docker/dockerfiles/linux/secure