cd $STAGE
unzip testdata/sampleSchemas.zip
cd sampleSchemas
command -v dos2unix && find  order_entry/2002/ -type f -exec dos2unix {} +
exit | sqlplus /nolog @mksample oracle oracle oracle oracle oracle oracle oracle oracle USERS TEMP log/ localhost:1521/$ORACLE_PDB
