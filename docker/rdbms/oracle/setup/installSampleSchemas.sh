cd $STAGE
unzip testdata/sampleSchemas.zip
cd sampleSchemas
# The purchase orders have CRLF line endings. On older versions (18c, 12.2, 11.2) DBMS_XDB.createResource normalises them to LF when it stores the document
# in OE.PURCHASEORDER; 23.26.1 EE keeps the CR, which MSSQL's XML type then normalises away (dbRoundtrip compare failures).
# Strip it here. The images have no dos2unix, so use sed.
find order_entry/2002/ -type f -exec sed -i 's/\r$//' {} +
exit | sqlplus /nolog @mksample oracle oracle oracle oracle oracle oracle oracle oracle USERS TEMP log/ localhost:1521/$ORACLE_PDB
