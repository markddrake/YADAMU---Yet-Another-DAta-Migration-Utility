drop database if exists Adventureworks;
go
drop database if exists AdventureWorks2017;
go
--
DECLARE @AdventureWorks2017         NVARCHAR(4000) = CONVERT(NVARCHAR(4000),SERVERPROPERTY('InstanceDefaultDataPath')) + '/AdventureWorks.mdf';
DECLARE @AdventureWorks2017_Log     NVARCHAR(4000) = CONVERT(NVARCHAR(4000),SERVERPROPERTY('InstanceDefaultDataPath')) + '/AdventureWorks.ldf';
--
RESTORE DATABASE AdventureWorks2017
FROM disk= '$(STAGE)/testdata/AdventureWorks2017.bak'
WITH 
MOVE 'AdventureWorks2017'         TO @AdventureWorks2017,
MOVE 'AdventureWorks2017_Log'     TO @AdventureWorks2017_Log,
REPLACE;
go
ALTER DATABASE Adventureworks2017 SET SINGLE_USER WITH ROLLBACK IMMEDIATE
GO
ALTER DATABASE Adventureworks2017 MODIFY NAME = Adventureworks
GO  
ALTER DATABASE Adventureworks SET MULTI_USER
GO
