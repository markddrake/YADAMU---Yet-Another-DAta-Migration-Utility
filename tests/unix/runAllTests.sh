. ../unix/initialize.sh $(readlink -f "$1")
. $YADAMU_SCRIPT_ROOT/unix/clone_MYSQL.sh
. $YADAMU_SCRIPT_ROOT/unix/clone_Oracle.sh
. $YADAMU_SCRIPT_ROOT/unix/clone_MSSQL.sh
. $YADAMU_SCRIPT_ROOT/unix/jTable_MYSQL.sh
. $YADAMU_SCRIPT_ROOT/unix/jTable_Oracle.sh
. $YADAMU_SCRIPT_ROOT/unix/jTable_MSSQL.sh
export MODE=DDL_AND_DATA
. $YADAMU_SCRIPT_ROOT/unix/clone_Oracle.sh
. $YADAMU_SCRIPT_ROOT/unix/jTable_Oracle.sh