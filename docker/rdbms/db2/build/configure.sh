source /database/config/db2inst1/sqllib/db2profile
db2 connect to YADAMU user DB2INST1 using oracle
db2 update db cfg for YADAMU using LOGFILSIZ 10240 
db2 update db cfg for YADAMU using APPLHEAPSZ AUTOMATIC
db2 update db cfg for yadamu using locklist 20480
db2 update db cfg for yadamu using maxlocks 30
# db2 update db cfg for yadamu using locktimeout 30
db2stop force 
db2start