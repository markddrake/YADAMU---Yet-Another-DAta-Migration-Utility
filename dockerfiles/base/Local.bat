docker rmi yadamu/base:latest
set DOCKER_BUILDKIT=1
docker build -t yadamu/base . -f Dockerfile
