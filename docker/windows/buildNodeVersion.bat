docker rmi -f yadamu/environment:latest
copy docker\dockerfiles\windows\environment.dockerignore .dockerignore
docker build --tag yadamu/environment:latest --build-arg "NODE_VERSION=%1" --file docker\dockerfiles\windows\nodeVersion .
del .dockerignore