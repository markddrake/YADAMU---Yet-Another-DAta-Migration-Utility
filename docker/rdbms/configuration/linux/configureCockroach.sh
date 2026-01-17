export CONTAINER_NAME=$1
docker exec -it                           $CONTAINER_NAME ./cockroach --host=$CONTAINER_NAME:26357 init --insecure
docker exec -it                           $CONTAINER_NAME mkdir -p /home/croach/stage
docker cp docker/rdbms/cockroach/setup    $CONTAINER_NAME:/home/croach/stage
docker exec -it                           $CONTAINER_NAME bash /home/croach/stage/setup/configure.sh