docker rmi -f yadamu/environment:latest
REM DOCKER_BUILDKIT is a Linux option only
set DOCKER_BUILDKIT=0
copy docker\dockerfiles\windows\environment.dockerignore .dockerignore
docker build -t  yadamu/environment:latest . -f docker\dockerfiles\windows\environment
del .dockerignore