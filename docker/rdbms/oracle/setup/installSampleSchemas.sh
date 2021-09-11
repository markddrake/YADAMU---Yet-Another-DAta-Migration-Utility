cd /opt/oracle/diag
unzip testdata/sampleSchemas.zip
cd /opt/oracle/diag/sampleSchemas
exit | sqlplus /nolog @mksample oracle oracle oracle oracle oracle oracle oracle oracle USERS TEMP log/ $ORACLE_PDB
