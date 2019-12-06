export ORAENV_ASK=NO
export ORACLE_SID=CDB19300
. oraenv
lsnrctl start
echo "startup;" | sqlplus -s  / as sysdba 
export ORACLE_SID=CDB18300
. oraenv
lsnrctl start
echo "startup;" | sqlplus -s  / as sysdba 
export ORACLE_SID=CDB12200
. oraenv
lsnrctl start
echo "startup;" | sqlplus -s  / as sysdba 
export ORACLE_SID=ORA11200
. oraenv
lsnrctl start
echo "startup;" | sqlplus -s  / as sysdba 
