RESTORE DATABASE AdventureWorks2017
FROM disk= '/var/opt/mssql/backup/AdventureWorks2017.bak'
WITH MOVE 'AdventureWorks2017' 
TO '/var/opt/mssql/data/AdventureWorks.mdf',
MOVE 'AdventureWorks2017_Log' 
TO '/var/opt/mssql/data/AdventureWorks.ldf'
,REPLACE
