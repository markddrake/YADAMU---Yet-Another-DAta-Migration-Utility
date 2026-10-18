# start the service

function _int() {
   echo "Stopping container."
   echo "SIGINT received, shutting down database!"
   sqlplus / as sysdba <<EOF
   shutdown immediate;
   exit;
EOF
   lsnrctl stop
}
trap _int SIGINT

function _term() {
   echo "Stopping container."
   echo "SIGTERM received, shutting down database!"
   sqlplus / as sysdba <<EOF
   shutdown immediate;
   exit;
EOF
   lsnrctl stop
}
trap _term SIGTERM

function _kill() {
   echo "SIGKILL received, shutting down database!"
   sqlplus / as sysdba <<EOF
   shutdown abort;
   exit;
EOF
   lsnrctl stop
}
trap _kill SIGKILL

# The database exists once its spfile is on the data volume (moved there below
# after the first dbca). Do not rely on /etc/oratab alone: this image skips
# root.sh (see dockerfile), so /etc/oratab is never written, and the check used
# to grep for CDB18300. Both sent every restart down the create path, where dbca
# fails on the existing datafiles and the database is never started.
DB_CREATED=0
if [ -f $ORACLE_BASE/oradata/dbconfig/spfile$ORACLE_SID.ora ] || grep -q "^$ORACLE_SID:" /etc/oratab 2>/dev/null
then
  DB_CREATED=1
fi
export DB_CREATED
if [ "$DB_CREATED" == "0" ]
then
  netca -silent -responseFile $ORACLE_HOME/assistants/netca/netca.rsp
  dbca -J"-Doracle.assistants.dbca.validate.ConfigurationParams=false" -silent -createDatabase -responseFile $ORACLE_HOME/dbca.rsp 
  mkdir $ORACLE_BASE/oradata/dbconfig/
  mv $ORACLE_HOME/dbs/spfile$ORACLE_SID.ora                 $ORACLE_BASE/oradata/dbconfig/
  mv $ORACLE_HOME/dbs/orapw$ORACLE_SID                      $ORACLE_BASE/oradata/dbconfig/
  ln -s $ORACLE_BASE/oradata/dbconfig/spfile$ORACLE_SID.ora $ORACLE_HOME/dbs/spfile$ORACLE_SID.ora
  ln -s $ORACLE_BASE/oradata/dbconfig/orapw$ORACLE_SID      $ORACLE_HOME/dbs/orapw$ORACLE_SID
  mv $ORACLE_HOME/network/admin/sqlnet.ora                  $ORACLE_BASE/oradata/dbconfig/
  mv $ORACLE_HOME/network/admin/listener.ora                $ORACLE_BASE/oradata/dbconfig/
  mv $ORACLE_HOME/network/admin/tnsnames.ora                $ORACLE_BASE/oradata/dbconfig/
  ln -s $ORACLE_BASE/oradata/dbconfig/sqlnet.ora            $ORACLE_HOME/network/admin/sqlnet.ora
  ln -s $ORACLE_BASE/oradata/dbconfig/listener.ora          $ORACLE_HOME/network/admin/listener.ora
  ln -s $ORACLE_BASE/oradata/dbconfig/tnsnames.ora          $ORACLE_HOME/network/admin/tnsnames.ora
  sqlplus -s / as sysdba <<-EOF
  alter Pluggable Database all save state
  /
EOF
else
  lsnrctl start
  sqlplus / as sysdba << EOF
   STARTUP;
   alter pluggable database all open;
   exit;
EOF
fi

LOWER_SID=`echo $ORACLE_SID | tr "[:upper:]" "[:lower:]"`
tail -f $ORACLE_BASE/diag/rdbms/$LOWER_SID/$ORACLE_SID/trace/alert_$ORACLE_SID.log &
childPID=$!
wait $childPID
