#!/bin/bash
# Set environment variables for YADAMU tests
# Lightweight - does NOT rebuild images or run initialization container

export YADAMU_HOME="${YADAMU_HOME:-/usr/src/YADAMU}"
export LOG_DIR="${LOG_DIR:-./logs}"

export CONTAINER_NAME="${CONTAINER_NAME:-YADAMU-01}"
export MSSQL12="${MSSQL12:-192.168.1.241}"
export MSSQL14="${MSSQL14:-192.168.1.235}"
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
docker rm -f $CONTAINER_NAME 2>/dev/null || true
