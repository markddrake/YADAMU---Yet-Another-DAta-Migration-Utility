docker rmi -f yadamu/environment:latest
copy docker\dockerfiles\windows\environment.dockerignore .dockerignore
docker build -t  yadamu/environment:latest . -f docker\dockerfiles\windows\environment
del .dockerignore