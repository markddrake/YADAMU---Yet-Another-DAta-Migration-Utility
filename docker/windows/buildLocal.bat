docker rmi -f yadamu/base:latest
REM DOCKER_BUILDKIT is a Linux option only
set DOCKER_BUILDKIT=0
copy docker\dockerfiles\windows\yadamu.dockerignore .dockerignore
docker build -t  yadamu/base:latest . -f docker/dockerfiles/windows/yadamu
del .dockerignore