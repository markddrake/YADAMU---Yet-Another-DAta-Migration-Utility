#!/bin/bash
set -e

# Run YADAMU Standard Test Suite with full initialization
# This rebuilds images and runs initialization container

# Full initialization (rebuilds images, runs init container)
. /usr/local/bin/initializeTests.sh 2>&1 | tee "$LOG_DIR/standardTests.log"

# Run standard tests
/usr/local/bin/standardTests.sh 2>&1 | tee -a "$LOG_DIR/standardTests.log"
