source  ~db2inst1/sqllib/db2profile
export STAGE=/database/stage
cd $STAGE
mkdir -p log
db2 -td/ -vmf sql/YADAMU_IMPORT.sql > log/YADAMU_IMPORT.log 2>&1
