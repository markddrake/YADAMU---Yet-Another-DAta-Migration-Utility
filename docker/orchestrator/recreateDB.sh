docker stop YADAMU-01
docker rm YADAMU-01
docker compose --file docker/dockerfiles/linux/docker-compose.yml down -v
docker compose --file docker/dockerfiles/linux/docker-compose.yml up -d
#startup 2025-12-14T23:38:32.230+00:00
#complete "2025-12-14T23:55:33.835+00:00"docker
echo "Waiting 20 minutes for Oracle DBCA to finish (19c is slow)"
sleep 1200
bash docker/rdbms/configuration/configureContainers.sh
