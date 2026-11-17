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

# The task ends with the SQL Server 2014 suites, which need the sql-server-2014 VM on yadamu-db3. Start it now if it's down;
# if it can't be started, the test container skips those suites (YADAMU_SKIP_TESTS, read by runRegressionTest.sh)
# instead of failing hundreds of connections.
. /usr/local/bin/ensureDb3.sh
if ! ensure_db3 mssql14 "$MSSQL14" 1433; then
    echo "ERROR: Skipping mssql2014ShortRegression, mssql2014DataTypes and mssql2014TestSuite."
    export YADAMU_SKIP_TESTS="${YADAMU_SKIP_TESTS:+$YADAMU_SKIP_TESTS }mssql2014ShortRegression mssql2014DataTypes mssql2014TestSuite"
fi

echo "Starting task YADAMU:$YADAMU_TASK_NAME using container $CONTAINER_NAME."
docker run \
    --security-opt=seccomp:unconfined \
    --name $CONTAINER_NAME \
    --memory="$MEMORY" \
    -v ${VOLUME}:/usr/src/YADAMU/mnt \
    --network $NETWORK \
    -e YADAMU_TEST_NAME=$YADAMU_TASK_NAME \
    -e YADAMU_THREADED \
    -e YADAMU_SKIP_TESTS \
    --add-host="MSSQL12-01:${MSSQL12}" \
    --add-host="MSSQL14-01:${MSSQL14}" \
    --add-host="TDATA17-01:${TERADATA}" \
    -d $IMAGE
	
echo "Tailing logs from $CONTAINER_NAME (Ctrl+C to exit)..."
docker logs -f $CONTAINER_NAME &    # Background process for viewing
LOG_PID=$!

docker wait $CONTAINER_NAME         # BLOCKS until container exits

kill $LOG_PID 2>/dev/null || true
echo "Conpleteed task YADAMU:$YADAMU_TASK_NAME using container $CONTAINER_NAME."
docker rm $CONTAINER_NAME 2>/dev/null || true