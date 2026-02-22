#!/bin/bash
set -e

# Source environment variables
. /usr/local/bin/setEnvironment.sh

echo "============================================"
echo "YADAMU QA Standard Test Suite"
echo "============================================"
echo ""

# Remove the test container
docker stop $CONTAINER_NAME 2>/dev/null || true
docker rm $CONTAINER_NAME 2>/dev/null || true

echo ""

export YADAMU_TASK_NAME=standard

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