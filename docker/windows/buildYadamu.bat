docker rmi -f yadamu/docker:latest
docker build -t  yadamu/docker:latest . -f docker/dockerfiles/windows/yadamuRuntime
docker rm RUNTIME-01
docker run --security-opt=seccomp:unconfined --name RUNTIME-01 --memory="16g" --network YADAMU-NET -d -vYADAMU_01-SHARED:/usr/src/YADAMU/mnt  yadamu/docker:latest
