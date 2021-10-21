docker rmi -f yadamu/regression:latest
REM DOCKER_BUILDKIT is a Linux option only
set DOCKER_BUILDKIT=0
copy docker\dockerfiles\windows\regression.dockerignore .dockerignore
docker build -t  yadamu/regression:latest . -f docker\dockerfiles\windows\regression
del .dockerignore