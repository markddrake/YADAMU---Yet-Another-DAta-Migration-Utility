docker compose --file docker\build\docker-compose.yml build base
docker compose --file docker\build\docker-compose.yml build regression
docker compose --file docker\build\docker-compose.yml build commandline
docker compose --file docker\build\docker-compose.yml build secure
docker compose --file docker\build\docker-compose.yml build service