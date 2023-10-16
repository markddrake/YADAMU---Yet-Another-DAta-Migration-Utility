#!/bin/bash
export YADAMU_SCRIPT_PATH=$(dirname "$0")
source $YADAMU_SCRIPT_PATH/linux/configureOracle.sh   ORA2303-01
# source $YADAMU_SCRIPT_PATH/linux/configureOracle.sh   ORA2103-01
source $YADAMU_SCRIPT_PATH/linux/configureOracle.sh   ORA1903-01
source $YADAMU_SCRIPT_PATH/linux/configureOracle.sh   ORA1803-01
source $YADAMU_SCRIPT_PATH/linux/configureOracle.sh   ORA1220-01
# source $YADAMU_SCRIPT_PATH/linux/configureOracle.sh   ORA1210-01
source $YADAMU_SCRIPT_PATH/linux/configureOracle.sh   ORA1120-01
# source $YADAMU_SCRIPT_PATH/linux/configureMySQL.sh    MYSQL80-01
source $YADAMU_SCRIPT_PATH/linux/configureMySQL.sh    MYSQL81-01
# source $YADAMU_SCRIPT_PATH/linux/configureMariaDB.sh  MARIA10-01
source $YADAMU_SCRIPT_PATH/linux/configureMariaDB.sh  MARIA11-01
# source $YADAMU_SCRIPT_PATH/linux/configureMsSQL.sh    MSSQL17-01
source $YADAMU_SCRIPT_PATH/linux/configureMsSQL.sh    MSSQL19-01
source $YADAMU_SCRIPT_PATH/linux/configureMsSQL.sh    MSSQL22-01
# source $YADAMU_SCRIPT_PATH/linux/configurePostgres.sh PGSQL12-01
# source $YADAMU_SCRIPT_PATH/linux/configurePostgres.sh PGSQL13-01
# source $YADAMU_SCRIPT_PATH/linux/configurePostgres.sh PGSQL14-01
# source $YADAMU_SCRIPT_PATH/linux/configurePostgres.sh PGSQL15-01
source $YADAMU_SCRIPT_PATH/linux/configurePostgres.sh PGSQL16-01
# source $YADAMU_SCRIPT_PATH/linux/configureMongoDB.sh  MONGO40-01
# source $YADAMU_SCRIPT_PATH/linux/configureMongoDB.sh  MONGO50-01
# source $YADAMU_SCRIPT_PATH/linux/configureMongoDB.sh  MONGO60-01
 source $YADAMU_SCRIPT_PATH/linux/configureMongoDB.sh  MONGO70-01
source $YADAMU_SCRIPT_PATH/linux/configureVertica.sh  VRTCA10-01
# source $YADAMU_SCRIPT_PATH/linux/configureVertica.sh  VRTCA11-01
source $YADAMU_SCRIPT_PATH/linux/configureVertica.sh  VRTCA12-01




