docker rmi -f yadamu/environment:latest
set DOCKER_BUILDKIT=1
docker build -t  yadamu/environment:latest . --build-arg "NODE_VERSION=%1" --file docker/dockerfiles/linux/nodeVersion --no-cache --progress=plain 
