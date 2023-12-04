docker rmi -f yadamu/base:latest
copy docker\dockerfiles\windows\yadamu.dockerignore .dockerignore
docker build -t  yadamu/base:latest . -f docker/dockerfiles/windows/yadamu
del .dockerignore