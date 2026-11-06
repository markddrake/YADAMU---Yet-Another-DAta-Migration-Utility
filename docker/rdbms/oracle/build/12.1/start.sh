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
# after the first dbca). /etc/oratab is in the container layer, so a container
# recreated on the existing data volume has no entry for the database, and dbca
# would fail on the existing datafiles.
DB_CREATED=0
if [ -f $ORACLE_BASE/oradata/dbconfig/spfile$ORACLE_SID.ora ] || grep -q "^$ORACLE_SID:" /etc/oratab 2>/dev/null
then
  DB_CREATED=1
fi
export DB_CREATED
if [ "$DB_CREATED" == "0" ]
then
  netca -silent -responseFile $ORACLE_HOME/assistants/netca/netca.rsp
  dbca -silent -createDatabase -responseFile $ORACLE_HOME/dbca.rsp 
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
  # The links created above live in the container layer, so a container recreated
  # on the existing data volume starts without them, and without the /etc/oratab
  # entry written by dbca.
  ln -sf $ORACLE_BASE/oradata/dbconfig/spfile$ORACLE_SID.ora $ORACLE_HOME/dbs/spfile$ORACLE_SID.ora
  ln -sf $ORACLE_BASE/oradata/dbconfig/orapw$ORACLE_SID      $ORACLE_HOME/dbs/orapw$ORACLE_SID
  ln -sf $ORACLE_BASE/oradata/dbconfig/sqlnet.ora            $ORACLE_HOME/network/admin/sqlnet.ora
  ln -sf $ORACLE_BASE/oradata/dbconfig/listener.ora          $ORACLE_HOME/network/admin/listener.ora
  ln -sf $ORACLE_BASE/oradata/dbconfig/tnsnames.ora          $ORACLE_HOME/network/admin/tnsnames.ora
  grep -q "^$ORACLE_SID:" /etc/oratab || echo "$ORACLE_SID:$ORACLE_HOME:N" >> /etc/oratab
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
