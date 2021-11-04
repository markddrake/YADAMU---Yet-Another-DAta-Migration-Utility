docker rmi -f yadamu/environment:latest
REM DOCKER_BUILDKIT is a Linux option only
set DOCKER_BUILDKIT=0
copy docker\dockerfiles\windows\environment.dockerignore .dockerignore
docker build --tag yadamu/environment:latest --build-arg "NODE_VERSION=%1" --file docker\dockerfiles\windows\nodeVersion .
del .dockerignore