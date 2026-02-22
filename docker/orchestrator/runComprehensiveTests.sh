#!/bin/bash
set -e

# Source initialize to get exports in current shell
. /usr/local/bin/initializeTests.sh 2>&1 | tee "$LOG_DIR/comprehensiveTests.log"

/usr/local/bin/standardTests.sh 2>&1 | tee -a "$LOG_DIR/comprehensiveTests.log"

/usr/local/bin/extendedTests.sh 2>&1 | tee -a "$LOG_DIR/comprehensiveTests.log"