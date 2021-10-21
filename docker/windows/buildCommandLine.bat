docker rmi -f yadamu/commandline:latest
set DOCKER_BUILDKIT=1
docker build -t  yadamu/commandline:latest . -f docker/dockerfiles/commandLine


