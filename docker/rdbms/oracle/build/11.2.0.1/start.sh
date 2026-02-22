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

export DB_CREATED=`cat /etc/oratab | grep ORA11200 | wc -l`
lsnrctl start
if [ "$DB_CREATED" == "0" ]
then
  dbca  -silent -createDatabase -responseFile $ORACLE_HOME/dbca.rsp 
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
  ln -s $ORACLE_BASE/oradata/dbconfig/tnsnames.ora          $ORACLE_HOME/network/admin/tnsnames.orasql
else
  sqlplus / as sysdba << EOF
   STARTUP;
   exit;
EOF
fi

LOWER_SID=`echo $ORACLE_SID | tr "[:upper:]" "[:lower:]"`
tail -f $ORACLE_BASE/diag/rdbms/$LOWER_SID/$ORACLE_SID/trace/alert_$ORACLE_SID.log &
childPID=$!
wait $childPID
