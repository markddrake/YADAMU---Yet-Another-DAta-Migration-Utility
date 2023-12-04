docker rmi -f yadamu/commandline:latest
copy docker\dockerfiles\windows\commandline.dockerignore .dockerignore
docker build -t  yadamu/commandline:latest . -f docker\dockerfiles\windows\commandLine
del .dockerignore