#!/bin/bash
set -e

# YADAMU Extended Test Suite Runner

# Source environment variables only if not already set
if [ -z "$YADAMU_HOME" ]; then
    if [ -f /usr/local/bin/setEnvironment.sh ]; then
        . /usr/local/bin/setEnvironment.sh
    fi
fi

# Configuration - these should match initializeTests.sh
: ${LOG_DIR:=./logs}
: ${MSSQL12:=192.168.1.234}
: ${MSSQL14:=192.168.1.235}
: ${GREEN:='\033[0;32m'}
: ${YELLOW:='\033[1;33m'}
: ${RED:='\033[0;31m'}
: ${NC:='\033[0m'}

# Database startup wait times
ORACLE_21C_STARTUP_WAIT=600
ORACLE_23AI_STARTUP_WAIT=60
ORACLE_26AI_STARTUP_WAIT=60
VERTICA_STARTUP_WAIT=300
DB2_STARTUP_WAIT=600
COCKROACH_STARTUP_WAIT=60
YUGABYTE_STARTUP_WAIT=60
SNOWFLAKE_STARTUP_WAIT=60

log_message() {
    echo -e "${GREEN}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1"
}

error_message() {
    echo -e "${RED}[$(date '+%Y-%m-%d %H:%M:%S')] ERROR:${NC} $1"
}

wait_message() {
    echo -e "${YELLOW}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1"
}

run_test_suite() {
    local suite_name=$1
    local log_file="$LOG_DIR/${suite_name}.log"
    
    log_message "Starting task YADAMU:$suite_name using container $CONTAINER_NAME."
	
    # Run the test suite script
    docker run \
        --security-opt=seccomp:unconfined \
        --name YADAMU-01 \
        --memory="16g" \
        -v YADAMU_01-SHARED:/usr/src/YADAMU/mnt \
        --network YADAMU-NET \
        -e YADAMU_TEST_NAME="$suite_name" \
        ${MSSQL12:+--add-host="MSSQL12-01:${MSSQL12}"} \
        ${MSSQL14:+--add-host="MSSQL14-01:${MSSQL14}"} \
        -d yadamu/secure:latest
    
    # Stream logs directly to stdout (captured by parent tee), redirect stderr too
    wait_message "Tailing logs from $CONTAINER_NAME (Ctrl+C to exit)..."
    docker logs -f YADAMU-01 2>&1 &
    LOG_PID=$!
    
    # Wait for container to complete
    wait_message "Tailing logs from $CONTAINER_NAME (Ctrl+C to exit)..."
    docker wait YADAMU-01 >/dev/null
    
    # Give backgrounded logs time to flush all output
    sleep 2
    kill $LOG_PID 2>/dev/null || true
    wait $LOG_PID 2>/dev/null || true
    
    docker rm YADAMU-01 2>/dev/null || true
    
    log_message "Conpleteed task YADAMU:$suite_name using container $CONTAINER_NAME (log: $log_file)."
}

run_cmdline_test() {
    log_message "Running Command Line Test Suite"
    docker run \
        --security-opt=seccomp:unconfined \
        --name YADAMU-01 \
        --memory="16g" \
        -v YADAMU_01-SHARED:/usr/src/YADAMU/mnt \
        --network YADAMU-NET \
        -e YADAMU_TEST_NAME="cmdLine" \
        ${MSSQL12:+--add-host="MSSQL12-01:${MSSQL12}"} \
        ${MSSQL14:+--add-host="MSSQL14-01:${MSSQL14}"} \
        -d yadamu/commandline:latest

    docker logs -f YADAMU-01 2>&1 &
    LOG_PID=$!
    wait_message "Waiting for Command Line Test Suite to complete..."
    docker wait YADAMU-01 >/dev/null
    sleep 2
    kill $LOG_PID 2>/dev/null || true
    wait $LOG_PID 2>/dev/null || true
    docker rm YADAMU-01 2>/dev/null || true
    log_message "Completed Command Line Test Suite"
}

compose_up() {
    local compose_file=$1
	local service=$2
    local service_name=$3
    
    log_message "Starting $service_name..."
    docker compose --file "$compose_file" up -d "$service"
}

compose_down() {
    local compose_file=$1
	local service=$2
    local service_name=$3
    
    log_message "Stopping $service_name..."
    docker compose --file "$compose_file" down -v "$service"
}

configure_database() {
    local db_type=$1
    local container_name=$2
    
    log_message "Configuring $db_type ($container_name)..."
    
    case "$db_type" in
        oracle)
            # Run the configuration script that's already in the orchestrator
            bash docker/rdbms/configuration/linux/configureOracle.sh "$container_name"
            ;;
        db2)
            bash docker/rdbms/configuration/linux/configureDB2.sh "$container_name"
            ;;
        cockroach)
            bash docker/rdbms/configuration/linux/configureCockroach.sh "$container_name"
            ;;
        yugabyte)
            # Yugabyte might not have a .sh yet - check if it exists
            if [ -f docker/rdbms/configuration/linux/configureYugabyte.sh ]; then
                bash docker/rdbms/configuration/linux/configureYugabyte.sh "$container_name"
            else
                error_message "configureYugabyte.sh not found - may need to create from .bat"
                return 1
            fi
            ;;
        vertica)
            # Vertica uses host vsql command, not a configure script
            # Copy SQL files and run via docker exec
            docker cp src/sql/vertica/YADAMU_IMPORT.sql "$container_name:/opt/vertica/YADAMU_IMPORT.sql"
            docker cp src/sql/vertica/YADAMU_COMPARE.sql "$container_name:/opt/vertica/YADAMU_COMPARE.sql"
            docker exec "$container_name" /opt/vertica/bin/vsql -Udbadmin -ddocker -f /opt/vertica/YADAMU_IMPORT.sql
            docker exec "$container_name" /opt/vertica/bin/vsql -Udbadmin -ddocker -f /opt/vertica/YADAMU_COMPARE.sql
            ;;
        snowflake)
            # Snowflake configuration runs via docker-compose
            # The configure.sh script is already set up to run on container start
            log_message "Snowflake configuration handled by container startup"
            ;;
        *)
            error_message "Unknown database type: $db_type"
            return 1
            ;;
    esac
    
    if [ $? -eq 0 ]; then
        log_message "Configuration complete for $db_type"
    else
        error_message "Configuration failed for $db_type"
        return 1
    fi
}

echo "============================================"
echo "YADAMU QA Extended Test Suite"
echo "============================================"
echo ""

# ============================================
# Individual Test Runner Function
# ============================================
# Usage: run_test <test_name>
# This function is called from run_all_tests() or can be called individually
run_test() {
    local test_name=$1
    
    if [ -z "$test_name" ]; then
        error_message "No test name provided to run_test function"
        exit 1
    fi
    
    echo "============================================"
    log_message "Running Test: $test_name"
    echo "============================================"
    
    case "$test_name" in
        cmdline)
            run_cmdline_test
            ;;
            
        oracle11g)
            run_test_suite "oracle11g"
            ;;
            
        oracle19c)
            run_test_suite "oracle19c"
            ;;
            
        oracle21c)
            compose_up "docker/rdbms/oracle/docker-compose.yml" "ORA2103-01" "Oracle 21c"
            
            # Tail Oracle container logs in background so they appear in orchestrator logs
            log_message "Monitoring Oracle 21c startup logs..."
            docker logs -f ORA2103-01 2>&1 &
            ORACLE_LOG_PID=$!
            
            wait_message "Waiting for Oracle 21c Startup (${ORACLE_21C_STARTUP_WAIT}s)..."
            sleep "$ORACLE_21C_STARTUP_WAIT"
            
            # Stop tailing logs
            kill $ORACLE_LOG_PID 2>/dev/null || true
            wait $ORACLE_LOG_PID 2>/dev/null || true
            
            configure_database "oracle" "ORA2103-01"
            run_test_suite "oracle21c"
            compose_down "docker/rdbms/oracle/docker-compose.yml" "ORA2103-01" "Oracle 21c"
            ;;
            
        oracle23ai)
            compose_up "docker/rdbms/oracle/docker-compose.yml" "ORACL23-01" "Oracle 23ai"
            
            # Tail Oracle container logs in background
            log_message "Monitoring Oracle 23ai startup logs..."
            docker logs -f ORACL23-01 2>&1 &
            ORACLE_LOG_PID=$!
            
            wait_message "Waiting for Oracle 23ai Startup (${ORACLE_23AI_STARTUP_WAIT}s)..."
            sleep "$ORACLE_23AI_STARTUP_WAIT"
            
            # Stop tailing logs
            kill $ORACLE_LOG_PID 2>/dev/null || true
            wait $ORACLE_LOG_PID 2>/dev/null || true
            
            configure_database "oracle" "ORACL23-01"
            run_test_suite "oracle23ai"
            compose_down "docker/rdbms/oracle/docker-compose.yml" "ORACL23-01" "Oracle 23ai"
            ;;
            
        oracle26ai)
            compose_up "docker/rdbms/oracle/docker-compose.yml" "ORACL26-01" "Oracle 26ai"
            
            # Tail Oracle container logs in background
            log_message "Monitoring Oracle 26ai startup logs..."
            docker logs -f ORACL26-01 2>&1 &
            ORACLE_LOG_PID=$!
            
            wait_message "Waiting for Oracle 26ai Startup (${ORACLE_26AI_STARTUP_WAIT}s)..."
            sleep "$ORACLE_26AI_STARTUP_WAIT"
            
            # Stop tailing logs
            kill $ORACLE_LOG_PID 2>/dev/null || true
            wait $ORACLE_LOG_PID 2>/dev/null || true
            
            configure_database "oracle" "ORACL26-01"
            run_test_suite "oracle26ai"
            compose_down "docker/rdbms/oracle/docker-compose.yml" "ORACL26-01" "Oracle 26ai"
            ;;

        vertica09)
            compose_up "docker/rdbms/vertica/docker-compose.yml" "VRTCA09-01" "Vertica 9"
            wait_message "Waiting for Vertica 9 Startup (${VERTICA_STARTUP_WAIT}s)..."
            sleep "$VERTICA_STARTUP_WAIT"
            configure_database "vertica" "VRTCA09-01"
            run_test_suite "vertica09"
            compose_down "docker/rdbms/vertica/docker-compose.yml" "VRTCA09-01" "Vertica 9"
            ;;
            
        db2)
            compose_up "docker/rdbms/db2/docker-compose.yml" "IBMDB2-12" "IBM DB2"
            wait_message "Waiting for IBM DB2 Startup (${DB2_STARTUP_WAIT}s)..."
            sleep "$DB2_STARTUP_WAIT"
            configure_database "db2" "IBMDB2-12"
            run_test_suite "db2"
            compose_down "docker/rdbms/db2/docker-compose.yml" "IBMDB2-12" "IBM DB2"
            ;;
            
        cockroach)
            compose_up "docker/rdbms/cockroach/docker-compose.yml" "ROACH01-01" "CockroachDB"
            wait_message "Waiting for CockroachDB Startup (${COCKROACH_STARTUP_WAIT}s)..."
            sleep "$COCKROACH_STARTUP_WAIT"
            configure_database "cockroach" "ROACH01-01"
            run_test_suite "cdb"
            compose_down "docker/rdbms/cockroach/docker-compose.yml" "ROACH01-01" "CockroachDB"
            ;;
            
        yugabyte)
            compose_up "docker/rdbms/yugabyte/docker-compose.yml" "YUGABYTE-01" "YugabyteDB"
            wait_message "Waiting for YugabyteDB Startup (${YUGABYTE_STARTUP_WAIT}s)..."
            sleep "$YUGABYTE_STARTUP_WAIT"
            configure_database "yugabyte" "YUGABYTE-01"
            run_test_suite "ydb"
            compose_down "docker/rdbms/yugabyte/docker-compose.yml" "YUGABYTE-01" "YugabyteDB"
            ;;
            
        snowflake)
            log_message "Building Snowflake orchestrator container..."
            docker compose --file docker/orchestrator/snowflake/docker-compose.yml build
            
															 
            compose_up "docker/orchestrator/snowflake/docker-compose.yml" "SNOWSQL-01"
            
            # Tail Snowflake container logs in background
            log_message "Monitoring Snowflake configuration logs..."
            docker logs -f SNOWSQL-01 2>&1 &
            SNOWFLAKE_LOG_PID=$!
            
            wait_message "Waiting for Snowflake configuration (${SNOWFLAKE_STARTUP_WAIT}s)..."
            sleep "$SNOWFLAKE_STARTUP_WAIT"
            
            # Stop tailing logs
            kill $SNOWFLAKE_LOG_PID 2>/dev/null || true
            wait $SNOWFLAKE_LOG_PID 2>/dev/null || true
            
            log_message "Snowflake helpers installed successfully"
            run_test_suite "snowflake"
			
            compose_down "docker/orchestrator/snowflake/docker-compose.yml" "SNOWSQL-01"
            ;;
            
        *)
            error_message "Unknown test: $test_name"
            echo "Available tests: cmdline, oracle11g, oracle19c, oracle21c, oracle23ai, oracle26ai, vertica09, db2, cockroach, yugabyte, snowflake"
            exit 1
            ;;
    esac
    
    echo ""
    log_message "Completed Test: $test_name"
    echo ""
}

# Main execution function - only called when running full suite
run_all_tests() {
    # Define the list of tests to run
    # This can be customized to exclude bitrot tests or run subsets
    EXTENDED_TESTS=(
        "cmdline"
        "oracle11g"
        "oracle19c"
        "oracle21c"
        "oracle23ai"
        "oracle26ai"
        "vertica09"
        "db2"
        "cockroach"
        "yugabyte"
        "snowflake"
    )

    # Iterate through all tests
    for test_name in "${EXTENDED_TESTS[@]}"; do
        run_test "$test_name"
    done
    
    # Test Suite Complete
    echo ""
    log_message "============================================"
    log_message "YADAMU Extended Test Suite Finished"
    log_message "============================================"
    echo ""
    echo "Test logs available in: $LOG_DIR"
    echo ""
    ls -lh "$LOG_DIR"/*.log
}

# Only run all tests if this script is executed directly (not sourced)
if [ "${BASH_SOURCE[0]}" = "${0}" ]; then
    run_all_tests
fi