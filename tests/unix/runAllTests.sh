. ../unix/initialize.sh $(readlink -f "$0")
sh $YADAMU_SCRIPT_ROOT/unix/clone_MYSQL.sh
sh $YADAMU_SCRIPT_ROOT/unix/clone_Oracle.sh 
sh $YADAMU_SCRIPT_ROOT/unix/clone_MSSQL.sh
sh $YADAMU_SCRIPT_ROOT/unix/jTable_MYSQL.sh
sh $YADAMU_SCRIPT_ROOT/unix/jTable_Oracle.sh
sh $YADAMU_SCRIPT_ROOT/unix/jTable_MSSQL.sh
export MODE=DDL_AND_DATA
sh $YADAMU_SCRIPT_ROOT/unix/clone_Oracle.sh 
sh $YADAMU_SCRIPT_ROOT/unix/jTable_Oracle.sh 