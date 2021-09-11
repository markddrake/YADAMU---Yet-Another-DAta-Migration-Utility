docker rmi -f yadamu/postgres:latest
set DOCKER_BUILDKIT=1
docker build -t  yadamu/postgres:latest . -f docker/dockerfiles/postgres
