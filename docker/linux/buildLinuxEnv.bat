docker rmi -f yadamu/environment:%1
set DOCKER_BUILDKIT=1
docker build -t yadamu/node:%1 . --build-arg "NODE_VERSION=%1" --file docker/dockerfiles/linux/linuxTestEnv --no-cache --progress=plain
REM docker-compose --file=docker/linux/TEST-ENV/docker-compose up -d -e NODE_VERSION=%1
docker run --name LINUX_CMDLINE --rm -it --network=host -v c:\Development\YADAMU:/usr/src/YADAMU -w /usr/src/YADAMU yadamu/node:%1