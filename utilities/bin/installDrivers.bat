set YADAMU_HOME=%CD%
cd oracle\node
rmdir /s node_modules
call npm install oracledb
call npm install uuid
call npm install wkx
cd %YADAMU_HOME%
cd mssql/node
rmdir /s node_modules
call npm install mssql
cd %YADAMU_HOME%
cd postgres/node
rmdir /s node_modules
call npm install pg
call npm install pg-copy-streams
call npm install pg-query-stream
cd %YADAMU_HOME%
cd mysql/node
rmdir /s node_modules
call npm install mysql
call npm install wkx
cd %YADAMU_HOME%
cd mariadb/node
rmdir /s node_modules
call npm install mariadb
cd %YADAMU_HOME%
cd mongodb/node
rmdir /s node_modules
call npm install mongodb
call npm install wkx
cd %YADAMU_HOME%
cd snowflake/node
rmdir /s node_modules
call npm install snowflake-sdk
call npm install wkx
cd %YADAMU_HOME%
