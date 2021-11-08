docker rmi -f yadamu/commandline:latest
REM DOCKER_BUILDKIT is a Linux option only
set DOCKER_BUILDKIT=0
copy docker\dockerfiles\windows\commandline.dockerignore .dockerignore
docker build -t  yadamu/commandline:latest . -f docker\dockerfiles\windows\commandLine
del .dockerignore