docker rmi -f yadamu/service:latest
copy docker\dockerfiles\windows\service.dockerignore .dockerignore
docker build -t yadamu/service . -f docker/dockerfiles/linux/service
del .dockerignore
