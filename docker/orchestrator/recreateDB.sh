docker stop YADAMU-01
docker rm YADAMU-01
docker compose --file docker/regressionTests/docker-compose.yml down -v --remove-orphans
docker compose --file docker/regressionTests/docker-compose.yml up -d
echo "Waiting for Oracle DBCA to finish"
sleep 1200
bash docker/rdbms/configuration/configureContainers.sh
