#!/bin/bash
#
# Sourced by standardTests.sh and extendedTests.sh.
#
#   ensure_db3 mssql12|mssql14|teradata <host> <port>
#
# Makes sure a database on yadamu-db3 (the SQL Server 2012/2014 VMs and Teradata) is up before the suites that use it.
# With the orchestrator's ssh key (DB3_SSH_DIR: id_ed25519 and known_hosts, mounted read-only by docker-compose.yml), it asks
# yadamu-db3 to start the database ("switchDatabase.sh ensure", the only thing the key's forced command allows; see
# docker/server/yadamu-db3/orchestratorCommand.sh) and waits for it. Without the key it only checks the port.
# Returns 0 when the database answers on <host>:<port>; otherwise prints one ERROR line and returns 1.
#
: ${DB3_HOST:=${TERADATA:-192.168.1.233}}
: ${DB3_USER:=yadamu}
: ${DB3_SSH_DIR:=/run/yadamu-db3}

ensure_db3() {
    local db=$1 host=$2 port=$3
    if [ -r "$DB3_SSH_DIR/id_ed25519" ] && [ -r "$DB3_SSH_DIR/known_hosts" ]; then
        echo "Asking $DB3_HOST to start $db if it isn't running"
        # A cold boot of a SQL Server VM or Teradata is slow, so allow a long timeout
        if ! timeout 1800 ssh -i "$DB3_SSH_DIR/id_ed25519" -o IdentitiesOnly=yes -o BatchMode=yes -o ConnectTimeout=15 \
                -o StrictHostKeyChecking=yes -o UserKnownHostsFile="$DB3_SSH_DIR/known_hosts" \
                "$DB3_USER@$DB3_HOST" ensure "$db"; then
            echo "ERROR: $DB3_HOST could not start $db (see the output above)."
            return 1
        fi
    fi
    if ! timeout 10 bash -c "</dev/tcp/$host/$port" 2>/dev/null; then
        echo "ERROR: $db is not answering on $host:$port. Start it on yadamu-db3 (switchDatabase.sh ensure $db)."
        return 1
    fi
}
