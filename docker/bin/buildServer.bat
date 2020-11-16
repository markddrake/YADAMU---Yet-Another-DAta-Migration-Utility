docker stop YADAMU-SVR
docker rm YADAMU-SVR
docker volume rm YADAMU_SVR_MNT
docker volume create YADAMU_SVR_MNT
docker rmi yadamu/server:latest
set DOCKER_BUILDKIT=1
docker build -t yadamu/server . -f docker/dockerfiles/yadamuServer
docker run --name YADAMU-SVR -v YADAMU_SVR_MNT:/usr/src/YADAMU/mnt --network YADAMU-NET -p3000:3000 -d yadamu/server:latest 
docker logs YADAMU-SVR

