#!/bin/bash
export YADAMU_SCRIPT_PATH=$(dirname "$0")
source $YADAMU_SCRIPT_PATH/linux/configureOracle.sh   ORA1903-01
source $YADAMU_SCRIPT_PATH/linux/configureOracle.sh   ORA1803-01
source $YADAMU_SCRIPT_PATH/linux/configureOracle.sh   ORA1220-01
source $YADAMU_SCRIPT_PATH/linux/configureOracle.sh   ORA1120-01
source $YADAMU_SCRIPT_PATH/linux/configureMsSQL.sh    MSSQL25-01
source $YADAMU_SCRIPT_PATH/linux/configureMsSQL.sh    MSSQL22-01
source $YADAMU_SCRIPT_PATH/linux/configurePostgres.sh PGSQL18-01
source $YADAMU_SCRIPT_PATH/linux/configureMySQL.sh    MYSQL95-01
source $YADAMU_SCRIPT_PATH/linux/configureMariaDB.sh  MARIA12-01
source $YADAMU_SCRIPT_PATH/linux/configureMongoDB.sh  MONGO80-01
source $YADAMU_SCRIPT_PATH/linux/configureVertica.sh  VRTCA24-01
source $YADAMU_SCRIPT_PATH/linux/configureVertica.sh  VRTCA10-01




