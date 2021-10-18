cd $STAGE
unzip testdata/sampleSchemas.zip
cd sampleSchemas
exit | sqlplus /nolog @mksample oracle oracle oracle oracle oracle oracle oracle oracle USERS TEMP log/ $ORACLE_PDB
