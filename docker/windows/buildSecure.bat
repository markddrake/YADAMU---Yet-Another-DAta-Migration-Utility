docker rmi -f yadamu/secure:latest
copy docker\dockerfiles\windows\secure.dockerignore .dockerignore
docker build -t  yadamu/secure:latest . -f docker\dockerfiles\windows\secure
del .dockerignore