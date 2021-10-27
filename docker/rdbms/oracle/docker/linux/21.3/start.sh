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

export DB_CREATED=`cat /etc/oratab | grep CDB21300 | wc -l`
if [ "$DB_CREATED" == "0" ]
then
  netca -silent -responseFile $ORACLE_HOME/assistants/netca/netca.rsp
  dbca -J"-Doracle.assistants.dbca.validate.ConfigurationParams=false" -silent -createDatabase -responseFile $ORACLE_HOME/dbca.rsp 
  mkdir -p $ORACLE_BASE/oradata/dbconfig/dbs
  mkdir -p $ORACLE_BASE/oradata/dbconfig/network/admin
  mv $ORACLE_BASE/dbs/spfile$ORACLE_SID.ora                       $ORACLE_BASE/oradata/dbconfig/dbs
  mv $ORACLE_BASE/dbs/orapw$ORACLE_SID                            $ORACLE_BASE/oradata/dbconfig/dbs
  ln -s $ORACLE_BASE/oradata/dbconfig/dbs/spfile$ORACLE_SID.ora   $ORACLE_BASE/dbs/spfile$ORACLE_SID.ora
  ln -s $ORACLE_BASE/oradata/dbconfig/dbs/orapw$ORACLE_SID        $ORACLE_BASE/dbs/orapw$ORACLE_SID
  mv $ORACLE_BASE/homes/OraDB21Home1/network/admin/sqlnet.ora     $ORACLE_BASE/oradata/dbconfig/network/admin
  mv $ORACLE_BASE/homes/OraDB21Home1/network/admin/listener.ora   $ORACLE_BASE/oradata/dbconfig/network/admin
  mv $ORACLE_BASE/homes/OraDB21Home1/network/admin/tnsnames.ora   $ORACLE_BASE/oradata/dbconfig/network/admin
  ln -s $ORACLE_BASE/oradata/dbconfig/network/admin/sqlnet.ora    $ORACLE_BASE/homes/OraDB21Home1/network/admin/sqlnet.ora
  ln -s $ORACLE_BASE/oradata/dbconfig/network/admin//listener.ora $ORACLE_BASE/homes/OraDB21Home1/network/admin/listener.ora
  ln -s $ORACLE_BASE/oradata/dbconfig/network/admin//tnsnames.ora $ORACLE_BASE/homes/OraDB21Home1/network/admin/tnsnames.ora
  sqlplus -s / as sysdba <<-EOF

  alter Pluggable Database all save state
  /
EOF
else
  lsnrctl start
  sqlplus / as sysdba << EOF
   STARTUP;
   exit;
EOF
fi
LOWER_SID=`echo $ORACLE_SID | tr "[:upper:]" "[:lower:]"`
tail -f $ORACLE_BASE/diag/rdbms/$LOWER_SID/$ORACLE_SID/trace/alert_$ORACLE_SID.log &
childPID=$!
wait $childPID
