#!/bin/bash
set -e

# YADAMU Individual Test Runner
# Runs a single test from the extended test suite

TEST_NAME=$1

if [ -z "$TEST_NAME" ]; then
    echo "Usage: runTest.sh <test_name>"
    echo ""
    echo "Available tests:"
    echo "  cmdline      - Command Line Test Suite"
    echo "  oracle11g    - Oracle 11g Test Suite"
    echo "  oracle19c    - Oracle 19c Test Suite"
    echo "  oracle21c    - Oracle 21c Test Suite (starts/stops container)"
    echo "  oracle23ai   - Oracle 23ai Test Suite (starts/stops container)"
    echo "  vertica09    - Vertica 9 Test Suite (starts/stops container)"
    echo "  db2          - IBM DB2 Test Suite (starts/stops container)"
    echo "  cdb          - CockroachDB Test Suite (starts/stops container)"
    echo "  ydb          - YugabyteDB Test Suite (starts/stops container)"
    exit 1
fi

# Source initialization
. /usr/local/bin/initializeTests.sh

# Configuration defaults
: ${LOG_DIR:=./logs}
: ${ORACLE_STARTUP_WAIT:=600}
: ${VERTICA_STARTUP_WAIT:=300}
: ${DB2_STARTUP_WAIT:=600}
: ${COCKROACH_STARTUP_WAIT:=60}
: ${YUGABYTE_STARTUP_WAIT:=60}
: ${GREEN:='\033[0;32m'}
: ${YELLOW:='\033[1;33m'}
: ${RED:='\033[0;31m'}
: ${NC:='\033[0m'}

log_message() {
    echo -e "${GREEN}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1"
}

error_message() {
    echo -e "${RED}[$(date '+%Y-%m-%d %H:%M:%S')] ERROR:${NC} $1"
}

wait_message() {
    echo -e "${YELLOW}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1"
}

run_yadamu_test() {
    local test_suite=$1
    local image=${2:-yadamu/secure:latest}
    
    log_message "Starting YADAMU test: $test_suite"
    
    docker run \
        --security-opt=seccomp:unconfined \
        --name YADAMU-01 \
        --memory="16g" \
        -v YADAMU_01-SHARED:/usr/src/YADAMU/mnt \
        --network YADAMU-NET \
        -e YADAMU_TEST_NAME="$test_suite" \
        ${MSSQL12:+--add-host="MSSQL12-01:${MSSQL12}"} \
        ${MSSQL14:+--add-host="MSSQL14-01:${MSSQL14}"} \
        -d "$image"
    
    docker logs -f YADAMU-01 2>&1 &
    LOG_PID=$!
    
    wait_message "Waiting for test to complete..."
    docker wait YADAMU-01 >/dev/null
    
    sleep 2
    kill $LOG_PID 2>/dev/null || true
    wait $LOG_PID 2>/dev/null || true
    
    docker rm YADAMU-01 2>/dev/null || true
    
    log_message "Completed YADAMU test: $test_suite"
}

start_oracle_21c() {
    log_message "Starting Oracle 21c container..."
    
    # Create volumes if they don't exist
    docker volume create ORA2103-01-DATA 2>/dev/null || true
    docker volume create ORA2103-01-DIAG 2>/dev/null || true
    
    docker run -d \
        --name ORA2103-01 \
        --hostname ORA2103-01 \
        --network YADAMU-NET \
        --memory 16g \
        --shm-size 4g \
        --security-opt seccomp:unconfined \
        --tmpfs /dev/shm:rw,nosuid,nodev,exec,size=4g \
        -p 15216:1521 \
        -e ORACLE_SID=CDB21300 \
        -e ORACLE_PDB=PDB21300 \
        -e ORACLE_PWD=oracle \
        -e ORACLE_CHARACTERSET=AL32UTF8 \
        -v YADAMU_01-SHARED:/mnt/shared \
        -v ORA2103-01-DATA:/opt/oracle/admin \
        -v ORA2103-01-DATA:/opt/oracle/oradata \
        -v ORA2103-01-DIAG:/opt/oracle/diag \
        yadamu/oracle:21.3.0
    
    wait_message "Waiting for Oracle 21c startup (${ORACLE_STARTUP_WAIT}s)..."
    sleep "$ORACLE_STARTUP_WAIT"
    
    configure_database "oracle" "ORA2103-01"
}

stop_oracle_21c() {
    log_message "Stopping Oracle 21c container..."
    docker stop ORA2103-01 2>/dev/null || true
    docker rm ORA2103-01 2>/dev/null || true
}

start_oracle_23ai() {
    log_message "Starting Oracle 23ai container..."
    
    # Create volumes if they don't exist
    docker volume create ORA2305-01-DATA 2>/dev/null || true
    docker volume create ORA2305-01-DIAG 2>/dev/null || true
    
    docker run -d \
        --name ORA2305-01 \
        --hostname ORA2305-01 \
        --network YADAMU-NET \
        --memory 16g \
        --shm-size 4g \
        --security-opt seccomp:unconfined \
        --tmpfs /dev/shm:rw,nosuid,nodev,exec,size=4g \
        -p 15218:1521 \
        -e ORACLE_SID=FREE \
        -e ORACLE_PDB=FREEPDB1 \
        -e ORACLE_PWD=oracle \
        -e ORACLE_PASSWORD=oracle \
        -e ORACLE_CHARACTERSET=AL32UTF8 \
        -v YADAMU_01-SHARED:/mnt/shared \
        -v ORA2305-01-DATA:/opt/oracle/admin \
        -v ORA2305-01-DATA:/opt/oracle/oradata \
        -v ORA2305-01-DIAG:/opt/oracle/diag \
        gvenzl/oracle-free:23.5-full
    
    wait_message "Waiting for Oracle 23ai startup (${ORACLE_STARTUP_WAIT}s)..."
    sleep "$ORACLE_STARTUP_WAIT"
    
    configure_database "oracle" "ORA2305-01"
}

stop_oracle_23ai() {
    log_message "Stopping Oracle 23ai container..."
    docker stop ORA2305-01 2>/dev/null || true
    docker rm ORA2305-01 2>/dev/null || true
}

start_vertica() {
    log_message "Starting Vertica container..."
    
    # Create volume if it doesn't exist
    docker volume create VRTCA09-01-DATA 2>/dev/null || true
    
    docker run -d \
        --name VRTCA09-01 \
        --hostname VRTCA09-01 \
        --network YADAMU-NET \
        --memory 12g \
        --shm-size 4g \
        --security-opt seccomp:unconfined \
        -p 54331:5433 \
        -p 54441:5444 \
        -v YADAMU_01-SHARED:/mnt/shared \
        -v VRTCA09-01-DATA:/home/dbadmin/docker \
        dataplatform/docker-vertica
    
    wait_message "Waiting for Vertica startup (${VERTICA_STARTUP_WAIT}s)..."
    sleep "$VERTICA_STARTUP_WAIT"
    
    configure_database "vertica" "VRTCA09-01"
}

stop_vertica() {
    log_message "Stopping Vertica container..."
    docker stop VRTCA09-01 2>/dev/null || true
    docker rm VRTCA09-01 2>/dev/null || true
}

start_db2() {
    log_message "Starting DB2 container..."
    
    # Create volume if it doesn't exist
    docker volume create IBMDB2-01-DATA 2>/dev/null || true
    
    docker run -d \
        --name IBMDB2-01 \
        --hostname IBMDB2-01 \
        --network YADAMU-NET \
        --memory 12g \
        --shm-size 4g \
        --security-opt label:disable \
        --privileged \
        -p 50000:50000 \
        -e DB2INST1_PASSWORD=oracle \
        -e LICENSE=accept \
        -e DBNAME=YADAMU \
        -e SAMPLEDB=true \
        -v YADAMU_01-SHARED:/mnt/shared \
        -v IBMDB2-01-DATA:/database \
        yadamu/db2
    
    wait_message "Waiting for DB2 startup (${DB2_STARTUP_WAIT}s)..."
    sleep "$DB2_STARTUP_WAIT"
    
    configure_database "db2" "IBMDB2-01"
}

stop_db2() {
    log_message "Stopping DB2 container..."
    docker stop IBMDB2-01 2>/dev/null || true
    docker rm IBMDB2-01 2>/dev/null || true
}

start_cockroach() {
    log_message "Starting CockroachDB 3-node cluster..."
    
    # Create volumes if they don't exist
    docker volume create ROACH01-01-DATA 2>/dev/null || true
    docker volume create ROACH01-02-DATA 2>/dev/null || true
    docker volume create ROACH01-03-DATA 2>/dev/null || true
    
    # Start node 1
    docker run -d \
        --name ROACH01-01 \
        --hostname ROACH01-01 \
        --network YADAMU-NET \
        --memory 8g \
        --shm-size 1g \
        --security-opt seccomp:unconfined \
        -p 26257:26257 \
        -p 8080:8080 \
        -v YADAMU_01-SHARED:/mnt/shared \
        -v ROACH01-01-DATA:/cockroach/cockroach-data \
        cockroachdb/cockroach \
        start --advertise-addr=ROACH01-01:26357 --listen-addr=ROACH01-01:26357 --http-addr=ROACH01-01:8080 --sql-addr=ROACH01-01:26257 --insecure --store=node1 --join=ROACH01-01,ROACH01-02,ROACH01-03
    
    # Start node 2
    docker run -d \
        --name ROACH01-02 \
        --hostname ROACH01-02 \
        --network YADAMU-NET \
        --memory 8g \
        --shm-size 1g \
        --security-opt seccomp:unconfined \
        -p 26258:26258 \
        -p 8081:8081 \
        -v YADAMU_01-SHARED:/mnt/shared \
        -v ROACH01-02-DATA:/cockroach/cockroach-data \
        cockroachdb/cockroach \
        start --advertise-addr=ROACH01-02:26357 --listen-addr=ROACH01-02:26357 --http-addr=ROACH01-02:8081 --sql-addr=ROACH01-02:26258 --insecure --join=ROACH01-01,ROACH01-02,ROACH01-03
    
    # Start node 3
    docker run -d \
        --name ROACH01-03 \
        --hostname ROACH01-03 \
        --network YADAMU-NET \
        --memory 8g \
        --shm-size 1g \
        --security-opt seccomp:unconfined \
        -p 26259:26259 \
        -p 8082:8082 \
        -v YADAMU_01-SHARED:/mnt/shared \
        -v ROACH01-03-DATA:/cockroach/cockroach-data \
        cockroachdb/cockroach \
        start --advertise-addr=ROACH01-03:26357 --listen-addr=ROACH01-03:26357 --http-addr=ROACH01-03:8082 --sql-addr=ROACH01-03:26259 --insecure --join=ROACH01-01,ROACH01-02,ROACH01-03
    
    wait_message "Waiting for CockroachDB cluster startup (${COCKROACH_STARTUP_WAIT}s)..."
    sleep "$COCKROACH_STARTUP_WAIT"
    
    configure_database "cockroach" "ROACH01-01"
}

stop_cockroach() {
    log_message "Stopping CockroachDB cluster..."
    docker stop ROACH01-01 ROACH01-02 ROACH01-03 2>/dev/null || true
    docker rm ROACH01-01 ROACH01-02 ROACH01-03 2>/dev/null || true
}

start_yugabyte() {
    log_message "Starting YugabyteDB container..."
    
    # Create volume if it doesn't exist
    docker volume create YUGABYTE_01-DATA 2>/dev/null || true
    
    docker run -d \
        --name YUGABYTE-01 \
        --hostname YUGABYTE-01 \
        --network YADAMU-NET \
        --memory 16g \
        -p 7000:7000 \
        -p 9010:9000 \
        -p 15433:15433 \
        -p 5533:5433 \
        -p 9042:9042 \
        -v YUGABYTE_01-DATA:/home/yugabyte/yb_data \
        -v YADAMU_01-SHARED:/mnt/shared \
        yugabytedb/yugabyte:2024.1.3.0-b105 \
        /home/yugabyte/bin/yugabyted start --background=false --base_dir=/home/yugabyte/yb_data
    
    wait_message "Waiting for YugabyteDB startup (${YUGABYTE_STARTUP_WAIT}s)..."
    sleep "$YUGABYTE_STARTUP_WAIT"
    
    configure_database "yugabyte" "YUGABYTE-01"
}

stop_yugabyte() {
    log_message "Stopping YugabyteDB container..."
    docker stop YUGABYTE-01 2>/dev/null || true
    docker rm YUGABYTE-01 2>/dev/null || true
}

configure_database() {
    local db_type=$1
    local container_name=$2
    
    log_message "Configuring $db_type ($container_name)..."
    
    case "$db_type" in
        oracle)
            bash /yadamu/docker/rdbms/configuration/linux/configureOracle.sh "$container_name"
            ;;
        db2)
            bash /yadamu/docker/rdbms/configuration/linux/configureDB2.sh "$container_name"
            ;;
        cockroach)
            bash /yadamu/docker/rdbms/configuration/linux/configureCockroach.sh "$container_name"
            ;;
        yugabyte)
            if [ -f /yadamu/docker/rdbms/configuration/linux/configureYugabyte.sh ]; then
                bash /yadamu/docker/rdbms/configuration/linux/configureYugabyte.sh "$container_name"
            else
                error_message "configureYugabyte.sh not found"
                return 1
            fi
            ;;
        vertica)
            docker cp /yadamu/src/sql/vertica/YADAMU_IMPORT.sql "$container_name:/opt/vertica/YADAMU_IMPORT.sql"
            docker cp /yadamu/src/sql/vertica/YADAMU_COMPARE.sql "$container_name:/opt/vertica/YADAMU_COMPARE.sql"
            docker exec "$container_name" vsql -Udbadmin -ddocker -f /opt/vertica/YADAMU_IMPORT.sql
            docker exec "$container_name" vsql -Udbadmin -ddocker -f /opt/vertica/YADAMU_COMPARE.sql
            ;;
    esac
}

# Main test execution
log_message "============================================"
log_message "Running Individual Test: $TEST_NAME"
log_message "============================================"

case "$TEST_NAME" in
    cmdline)
        run_yadamu_test "cmdLine" "yadamu/commandline:latest"
        ;;
    
    oracle11g)
        run_yadamu_test "oracle11g"
        ;;
    
    oracle19c)
        run_yadamu_test "oracle19c"
        ;;
    
    oracle21c)
        start_oracle_21c
        run_yadamu_test "oracle21c"
        stop_oracle_21c
        ;;
    
    oracle23ai)
        start_oracle_23ai
        run_yadamu_test "oracle23ai"
        stop_oracle_23ai
        ;;
    
    vertica09)
        start_vertica
        run_yadamu_test "vertica09"
        stop_vertica
        ;;
    
    db2)
        start_db2
        run_yadamu_test "db2"
        stop_db2
        ;;
    
    cdb)
        start_cockroach
        run_yadamu_test "cdb"
        stop_cockroach
        ;;
    
    ydb)
        start_yugabyte
        run_yadamu_test "ydb"
        stop_yugabyte
        ;;
    
    *)
        error_message "Unknown test: $TEST_NAME"
        exit 1
        ;;
esac

log_message "============================================"
log_message "Test Complete: $TEST_NAME"
log_message "============================================"
