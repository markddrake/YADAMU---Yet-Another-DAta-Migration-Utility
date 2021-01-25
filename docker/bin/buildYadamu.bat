docker rmi -f yadamu/docker:latest
set DOCKER_BUILDKIT=1
docker build -t  yadamu/docker:latest . -f docker/dockerfiles/yadamuRuntime
docker rm RUNTIME-01
docker run --name RUNTIME-01 --memory="16g" --network YADAMU-NET -d -vYADAMU_01_MNT:/usr/src/YADAMU/mnt  yadamu/docker:latest
