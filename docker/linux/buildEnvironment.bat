docker rmi -f yadamu/environment:latest
set DOCKER_BUILDKIT=1
docker build -t  yadamu/environment:latest . -f docker/dockerfiles/linux/environment --no-cache --progress=plain
