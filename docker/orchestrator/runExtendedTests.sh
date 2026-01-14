#!/bin/bash
set -e

# Run YADAMU Extended Test Suite with full initialization
# This rebuilds images and runs initialization container

# Full initialization (rebuilds images, runs init container)
. /usr/local/bin/initializeTests.sh 2>&1 | tee "$LOG_DIR/extendedTests.log"

# Run extended tests (this calls run_all_tests function)
/usr/local/bin/extendedTests.sh 2>&1 | tee -a "$LOG_DIR/extendedTests.log"
