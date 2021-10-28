drop database if exists AdventureWorksDW;
go
drop database if exists AdventureWorksDW2017;
go
--
DECLARE @AdventureWorksDW2017         NVARCHAR(4000) = CONVERT(NVARCHAR(4000),SERVERPROPERTY('InstanceDefaultDataPath')) + '/AdventureWorksDW.mdf';
DECLARE @AdventureWorksDW2017_Log     NVARCHAR(4000) = CONVERT(NVARCHAR(4000),SERVERPROPERTY('InstanceDefaultDataPath')) + '/AdventureWorksDW.ldf';
--
RESTORE DATABASE AdventureWorksDW2017
FROM disk= '$(STAGE)/testdata/AdventureWorksDW2017.bak'
WITH 
MOVE 'AdventureWorksDW2017'         TO @AdventureWorksDW2017,
MOVE 'AdventureWorksDW2017_Log'     TO @AdventureWorksDW2017_Log,
REPLACE;
go
ALTER DATABASE AdventureworksDW2017 SET SINGLE_USER WITH ROLLBACK IMMEDIATE
GO
ALTER DATABASE AdventureworksDW2017 MODIFY NAME = AdventureworksDW
GO  
ALTER DATABASE AdventureworksDW SET MULTI_USER
GO

