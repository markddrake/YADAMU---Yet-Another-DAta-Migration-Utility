docker rmi -f yadamu/wtf:latest
set DOCKER_BUILDKIT=1
docker build -t  yadamu/wtf:latest . -f docker/dockerfiles/linux/yadamuWTF


