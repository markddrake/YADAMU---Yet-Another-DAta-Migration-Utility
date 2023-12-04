docker rmi -f yadamu/regression:latest
copy docker\dockerfiles\windows\regression.dockerignore .dockerignore
docker build -t  yadamu/regression:latest . -f docker\dockerfiles\windows\regression
del .dockerignore