docker rmi -f yadamu/snowflake:latest
set DOCKER_BUILDKIT=1
docker build -t  yadamu/snowflake:latest . -f docker/dockerfiles/snowflake


