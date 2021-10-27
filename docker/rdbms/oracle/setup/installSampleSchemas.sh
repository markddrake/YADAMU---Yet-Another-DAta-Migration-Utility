cd $STAGE
unzip testdata/sampleSchemas.zip
cd sampleSchemas
exit | sqlplus /nolog @mksample oracle oracle oracle oracle oracle oracle oracle oracle USERS TEMP log/ localhost:1521/$ORACLE_PDB
