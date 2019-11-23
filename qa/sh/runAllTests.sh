. ../sh/initialize.sh $(readlink -f "$1")
. $YADAMU_SCRIPT_ROOT/sh/import_MYSQL.sh
. $YADAMU_SCRIPT_ROOT/sh/import_Oracle.sh
. $YADAMU_SCRIPT_ROOT/sh/import_MSSQL.sh
. $YADAMU_SCRIPT_ROOT/sh/upload_MYSQL.sh
. $YADAMU_SCRIPT_ROOT/sh/upload_Oracle.sh
. $YADAMU_SCRIPT_ROOT/sh/upload_MSSQL.sh
export MODE=DDL_AND_DATA
. $YADAMU_SCRIPT_ROOT/sh/import_Oracle.sh
. $YADAMU_SCRIPT_ROOT/sh/upload_Oracle.sh