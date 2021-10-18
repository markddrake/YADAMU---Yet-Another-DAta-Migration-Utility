drop database if exists WorldWideImportersDW;
go
--
DECLARE @WWI_Primary         NVARCHAR(4000) = CONVERT(NVARCHAR(4000),SERVERPROPERTY('InstanceDefaultDataPath')) + '/WideWorldImportersDW.mdf';
DECLARE @WWI_UserData        NVARCHAR(4000) = CONVERT(NVARCHAR(4000),SERVERPROPERTY('InstanceDefaultDataPath')) + '/WideWorldImporters_UserDataDW.mdf';
DECLARE @WWI_Log             NVARCHAR(4000) = CONVERT(NVARCHAR(4000),SERVERPROPERTY('InstanceDefaultDataPath')) + '/WideWorldImportersDW.ldf';
DECLARE @WWI_InMemory_Data_1 NVARCHAR(4000) = CONVERT(NVARCHAR(4000),SERVERPROPERTY('InstanceDefaultDataPath')) + '/WideWorldImportersDW_InMemory_Data_1';
--
RESTORE DATABASE WorldWideImportersDW
FROM disk= '$(STAGE)/testdata/WideWorldImportersDW-Full.bak'
WITH 
MOVE 'WWI_Primary'         TO @WWI_Primary,
MOVE 'WWI_UserData'        TO @WWI_UserData,
MOVE 'WWI_Log'             TO @WWI_Log,
MOVE 'WWIDW_InMemory_Data_1' TO @WWI_InMemory_Data_1,
REPLACE;
go
