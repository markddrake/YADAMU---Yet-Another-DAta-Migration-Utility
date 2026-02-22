#!/bin/bash
set -e

# Wrapper to run individual test from extendedTests.sh
# Usage: runIndividualTest.sh <test_name>

TEST_NAME=$1

if [ -z "$TEST_NAME" ]; then
    echo "Usage: $0 <test_name>"
    echo "Available tests: cmdline, oracle11g, oracle19c, oracle21c, oracle23ai, vertica09, db2, cockroach, yugabyte"
    exit 1
fi

# Set environment variables only (no image rebuilds, no init container)
. /usr/local/bin/setEnvironment.sh

# Source extendedTests.sh to get all functions (this won't execute main code anymore)
. /usr/local/bin/extendedTests.sh

# Now call run_test function with logging
run_test "$TEST_NAME" 2>&1 | tee "$LOG_DIR/${TEST_NAME}.log"
