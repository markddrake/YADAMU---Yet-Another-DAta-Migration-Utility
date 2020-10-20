docker rmi -f yadamu/base:latest
set DOCKER_BUILDKIT=1
docker build https://github.com/markddrake/YADAMU---Yet-Another-DAta-Migration-Utility.git#master  -t yadamu/base
