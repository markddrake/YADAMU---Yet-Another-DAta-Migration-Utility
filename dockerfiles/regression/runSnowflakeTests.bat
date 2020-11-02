docker rm SFLAKE-01
docker run --name SFLAKE-01 --memory="16g" -v YADAMU_01_MNT:/usr/src/YADAMU/mnt --network YADAMU-NET -d -e YADAMU_TEST_NAME=snowflake yadamu/snowflake:latest
docker logs SFLAKE-01
docker cp SFLAKE-01:/usr/src/YADAMU/mnt/log .
