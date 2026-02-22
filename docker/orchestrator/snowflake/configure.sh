#!/bin/bash

# Snowflake Helper Installation Script for YADAMU
# Reads credentials from secureConnections.json

set -e  # Exit on error

# Configuration
SECURE_CONNECTIONS="${SECURE_CONNECTIONS:-./secureConnections.json}"
SCRIPT_DIR="${SCRIPT_DIR:-./sql}"
SNOWFLAKE_CONFIG="${SNOWFLAKE_CONFIG:-snow#1}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if jq is available
if ! command -v jq &> /dev/null; then
    log_error "jq is required but not installed. Install with: apt-get install jq"
    exit 1
fi

# Check if secureConnections.json exists
if [ ! -f "$SECURE_CONNECTIONS" ]; then
    log_error "secureConnections.json not found at: $SECURE_CONNECTIONS"
    exit 1
fi

# Configuration name (defaults to snow#1)
SNOWFLAKE_CONFIG="${SNOWFLAKE_CONFIG:-snow#1}"

# Extract Snowflake credentials from named configuration
SNOWFLAKE_ACCOUNT=$(jq -r --arg cfg "$SNOWFLAKE_CONFIG" '.[$cfg].snowflake.account // empty' "$SECURE_CONNECTIONS")
SNOWFLAKE_USER=$(jq -r --arg cfg "$SNOWFLAKE_CONFIG" '.[$cfg].snowflake.username // .[$cfg].snowflake.user // empty' "$SECURE_CONNECTIONS")
SNOWFLAKE_TOKEN=$(jq -r --arg cfg "$SNOWFLAKE_CONFIG" '.[$cfg].snowflake.token // empty' "$SECURE_CONNECTIONS")
SNOWFLAKE_PASSWORD=$(jq -r --arg cfg "$SNOWFLAKE_CONFIG" '.[$cfg].snowflake.password // empty' "$SECURE_CONNECTIONS")
SNOWFLAKE_WAREHOUSE=$(jq -r --arg cfg "$SNOWFLAKE_CONFIG" '.[$cfg].snowflake.warehouse // "DEMO_WH"' "$SECURE_CONNECTIONS")
SNOWFLAKE_DATABASE=$(jq -r --arg cfg "$SNOWFLAKE_CONFIG" '.[$cfg].snowflake.database // "YADAMU_SYSTEM"' "$SECURE_CONNECTIONS")

# Check required credentials
if [ -z "$SNOWFLAKE_ACCOUNT" ] || [ -z "$SNOWFLAKE_USER" ]; then
    log_error "Missing required Snowflake credentials in secureConnections.json"
    log_error "Looking for configuration: $SNOWFLAKE_CONFIG"
    log_error "Expected format (PAT):"
    log_error '  "snow#1": {'
    log_error '    "snowflake": {'
    log_error '      "account": "SNOWFLAKE_ACCOUNT",'
    log_error '      "username": "SNOWFLAKE_USER",'
    log_error '      "token": "SNOWFLAKE_TOKEN",'
    log_error '      "warehouse": "SNOWFLAKE_WAREHOUSE"'
    log_error '    }'
    log_error '  }'
    log_error ""
    log_error "Available configurations:"
    jq -r 'keys[] | select(startswith("snow"))' "$SECURE_CONNECTIONS"
    exit 1
fi

# Validate authentication method
# PATs (Personal Access Tokens) are used as passwords in SnowSQL
if [ -n "$SNOWFLAKE_TOKEN" ]; then
    log_info "Using Personal Access Token (PAT) authentication"
    AUTH_VALUE="$SNOWFLAKE_TOKEN"
elif [ -n "$SNOWFLAKE_PASSWORD" ]; then
    log_info "Using password authentication (consider switching to PAT)"
    AUTH_VALUE="$SNOWFLAKE_PASSWORD"
else
    log_error "No valid authentication method found (need token or password)"
    log_error "Expected format (PAT):"
    log_error '  "snow#1": {'
    log_error '    "snowflake": {'
    log_error '      "account": "SNOWFLAKE_ACCOUNT",'
    log_error '      "username": "SNOWFLAKE_USER",'
    log_error '      "token": "SNOWFLAKE_TOKEN",'
    log_error '      "warehouse": "SNOWFLAKE_WAREHOUSE"'
    log_error '    }'
    log_error '  }'
    exit 1
fi

# Create SnowSQL config file
mkdir -p ~/.snowsql

cat > ~/.snowsql/config <<EOF
[connections]
accountname = $SNOWFLAKE_ACCOUNT
username = $SNOWFLAKE_USER
password = $AUTH_VALUE
warehousename = $SNOWFLAKE_WAREHOUSE
dbname = YADAMU_SYSTEM

[options]
exit_on_error = True
quiet = False
EOF

log_info "Installing YADAMU helpers to Snowflake account: $SNOWFLAKE_ACCOUNT (using $SNOWFLAKE_CONFIG)"

# Create database
log_info "Creating YADAMU_SYSTEM database..."
snowsql -q "CREATE TRANSIENT DATABASE IF NOT EXISTS YADAMU_SYSTEM DATA_RETENTION_TIME_IN_DAYS = 0;"

# Install helper procedures
log_info "Installing helper procedures from YADAMU_INSTALL.sql..."
if [ -f "$SCRIPT_DIR/YADAMU_INSTALL.sql" ]; then
    snowsql -f "$SCRIPT_DIR/YADAMU_INSTALL.sql"
    snowsql -q "CALL YADAMU_SYSTEM.PUBLIC.INSTALL_HELPERS();"
else
    log_error "YADAMU_INSTALL.sql not found at $SCRIPT_DIR/YADAMU_INSTALL.sql"
    exit 1
fi

# Install comparison procedures
log_info "Installing comparison procedures from YADAMU_COMPARE.sql..."
if [ -f "$SCRIPT_DIR/YADAMU_COMPARE.sql" ]; then
    snowsql -f "$SCRIPT_DIR/YADAMU_COMPARE.sql"
else
    log_error "YADAMU_COMPARE.sql not found at $SCRIPT_DIR/YADAMU_COMPARE.sql"
    exit 1
fi

# Verify installation
log_info "Verifying installation..."
snowsql -q "CALL YADAMU_SYSTEM.PUBLIC.YADAMU_INSTANCE_ID();"

log_info "✓ Installation complete!"