drop database if exists WorldWideImporters;
go
--
DBCC TRACEON (9830, -1);
go
--
DECLARE @WWI_Primary         NVARCHAR(4000) = CONVERT(NVARCHAR(4000),SERVERPROPERTY('InstanceDefaultDataPath')) + '/WideWorldImporters.mdf';
DECLARE @WWI_UserData        NVARCHAR(4000) = CONVERT(NVARCHAR(4000),SERVERPROPERTY('InstanceDefaultDataPath')) + '/WideWorldImporters_UserData.mdf';
DECLARE @WWI_Log             NVARCHAR(4000) = CONVERT(NVARCHAR(4000),SERVERPROPERTY('InstanceDefaultDataPath')) + '/WideWorldImporters.ldf';
DECLARE @WWI_InMemory_Data_1 NVARCHAR(4000) = CONVERT(NVARCHAR(4000),SERVERPROPERTY('InstanceDefaultDataPath')) + '/WideWorldImporters_InMemory_Data_1';
--
RESTORE DATABASE WorldWideImporters
FROM disk= '$(STAGE)/testdata/WideWorldImporters-Full.bak'
WITH 
MOVE 'WWI_Primary'         TO @WWI_Primary,
MOVE 'WWI_UserData'        TO @WWI_UserData,
MOVE 'WWI_Log'             TO @WWI_Log,
MOVE 'WWI_InMemory_Data_1' TO @WWI_InMemory_Data_1,
REPLACE;
go
