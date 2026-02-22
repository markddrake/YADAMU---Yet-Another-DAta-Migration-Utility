#!/bin/bash
set -e

export DOCKER_CONTEXT="${1:-default}"
export YADAMU_HOME="${YADAMU_HOME:-/usr/src/YADAMU}"
export LOG_DIR="${LOG_DIR:-./logs}"

export CONTAINER_NAME="${CONTAINER_NAME:-YADAMU-01}"
export MSSQL12=192.168.1.234
export MSSQL14=192.168.1.235
export MEMORY="${MEMORY:-16g}"
export NETWORK="${NETWORK:-YADAMU-NET}"
export VOLUME="${VOLUME:-YADAMU_01-SHARED}"
export IMAGE="${IMAGE:-yadamu/secure:latest}"


# Colors for output
export GREEN='\033[0;32m'
export YELLOW='\033[1;33m'
export RED='\033[0;31m'
export NC='\033[0m' # No Color

# Create log directory
mkdir -p "$LOG_DIR"


echo "============================================"
echo "YADAMU QA Test Suite Initialization"
echo "============================================"
echo "Container Name: $CONTAINER_NAME"
echo "Log Directory: $LOG_DIR"
echo "SQL Server 2012: $MSSQL12"
echo "SQL Server 2014: $MSSQL14"
echo "Memory Limit: $MEMORY"
echo "Network: $NETWORK"
echo "Image: $IMAGE"
echo "============================================"

# Clean up any existing container
echo "Cleaning up existing container..."

# docker stop $CONTAINER_NAME 
docker rm -f $CONTAINER_NAME 2>/dev/null || true


set DOCKER_BUILDKIT=1
docker rmi -f yadamu/base:latest
docker rmi -f yadamu/regression:latest
docker rmi -f yadamu/secure:latest
docker rmi -f yadamu/commandline:latest
docker rmi -f yadamu/service:latest
docker compose --file docker/build/docker-compose.yml  build base
docker compose --file docker/build/docker-compose.yml  build regression
docker compose --file docker/build/docker-compose.yml  build secure
docker compose --file docker/build/docker-compose.yml  build commandline
docker compose --file docker/build/docker-compose.yml  build service
docker cp /usr/local/bin/clean.sh YADAMU-SMB:/mount
docker cp /usr/local/bin/status.sh YADAMU-SMB:/mount
docker exec    YADAMU-SMB sh /mount/clean.sh

echo "============================================"
echo "YADAMU QA Standard Test Suite: Initilization"
echo "============================================"
echo ""

# Remove  the test container
docker stop $CONTAINER_NAME 2>/dev/null || true
docker rm $CONTAINER_NAME 2>/dev/null || true

export YADAMU_TASK_NAME=initialization

echo "Starting task YADAMU:$YADAMU_TASK_NAME using container $CONTAINER_NAME."
docker run \
    --security-opt=seccomp:unconfined \
    --name $CONTAINER_NAME \
    --memory="$MEMORY" \
    -v ${VOLUME}:/usr/src/YADAMU/mnt \
    --network $NETWORK \
    -e YADAMU_TEST_NAME=$YADAMU_TASK_NAME \
    --add-host="MSSQL12-01:${MSSQL12}" \
    --add-host="MSSQL14-01:${MSSQL14}" \
    -d $IMAGE
	
echo "Tailing logs from $CONTAINER_NAME (Ctrl+C to exit)..."
docker logs -f $CONTAINER_NAME &    # Background process for viewing
LOG_PID=$!

docker wait $CONTAINER_NAME         # BLOCKS until container exits

kill $LOG_PID 2>/dev/null || true
echo "Conpleteed task YADAMU:$YADAMU_TASK_NAME using container $CONTAINER_NAME."
docker rm $CONTAINER_NAME 2>/dev/null || true