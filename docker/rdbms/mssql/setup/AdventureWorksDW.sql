RESTORE DATABASE AdventureWorksDW
FROM disk= '/var/opt/mssql/testdata/AdventureWorksDW2017.bak'
WITH MOVE 'AdventureWorksDW2017'
TO '/var/opt/mssql/data/AdventureWorksDW.mdf',
MOVE 'AdventureWorksDW2017_Log' 
TO '/var/opt/mssql/data/AdventureWorksDW.ldf',
REPLACE
