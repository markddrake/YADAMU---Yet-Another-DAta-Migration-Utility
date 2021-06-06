RESTORE DATABASE AdventureWorks2017
FROM disk= '/var/opt/mssql/testdata/AdventureWorks2017.bak'
WITH MOVE 'AdventureWorks2017' 
TO '/var/opt/mssql/data/AdventureWorks.mdf',
MOVE 'AdventureWorks2017_Log' 
TO '/var/opt/mssql/data/AdventureWorks.ldf'
,REPLACE
go
ALTER DATABASE Adventureworks2017 SET SINGLE_USER WITH ROLLBACK IMMEDIATE
GO
ALTER DATABASE Adventureworks2017 MODIFY NAME = Adventureworks
GO  
ALTER DATABASE Adventureworks SET MULTI_USER
GO
