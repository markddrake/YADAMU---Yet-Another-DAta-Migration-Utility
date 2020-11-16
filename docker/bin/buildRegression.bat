docker rmi -f yadamu/regression:latest
set DOCKER_BUILDKIT=1
docker build -t  yadamu/regression:latest . -f docker/dockerfiles/regression
