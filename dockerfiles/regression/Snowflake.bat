docker stop SFLAKE-01
docker rm SFLAKE-01
docker rmi -f yadamu/snowflake:latest
set DOCKER_BUILDKIT=1
docker build -t  yadamu/snowflake:latest . -f dockerfiles/regression/Snowflake
docker create --name SFLAKE-01 --memory="16g" -v YADAMU_01_MNT:/usr/src/YADAMU/mnt --network YADAMU-NET yadamu/snowflake:latest 
rem docker cp JSON SFLAKE-01:/usr/src/YADAMU/mnt


