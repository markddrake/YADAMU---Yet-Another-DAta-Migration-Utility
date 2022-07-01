docker rmi -f yadamu/wtf
set DOCKER_BUILDKIT=1
docker build -t yadamu/wtf . --build-arg "NODE_VERSION=latest" --file docker/dockerfiles/linux/linuxTestEnv --no-cache --progress=plain
docker run --name LINUX_WTF --rm -it --network=host -v c:\Development\WTF-YADAMU:/usr/src/YADAMU -w /usr/src/YADAMU yadamu/wtf