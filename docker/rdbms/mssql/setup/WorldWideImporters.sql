drop database if exists WorldWideImporters;
go
--
DECLARE @DEFAULT_DATA_PATH   NVARCHAR(4000) = CONVERT(NVARCHAR(4000),SERVERPROPERTY('InstanceDefaultDataPath'))
DECLARE @WWI_Primary         NVARCHAR(4000) = @DEFAULT_DATA_PATH + 'WideWorldImporters.mdf';
DECLARE @WWI_UserData        NVARCHAR(4000) = @DEFAULT_DATA_PATH + 'WideWorldImporters_UserData.mdf';
DECLARE @WWI_Log             NVARCHAR(4000) = @DEFAULT_DATA_PATH + 'WideWorldImporters.ldf';
DECLARE @WWI_InMemory_Data_1 NVARCHAR(4000) = @DEFAULT_DATA_PATH + 'WideWorldImporters_InMemory_Data_1';
DECLARE @HOST_PLATFORM       NVARCHAR(8);
SELECT @HOST_PLATFORM = host_platform FROM sys.dm_os_host_info;
-- SQL Server 2017 on Windows has problem with sotring In-Memory data on a Docker Volume (Symbolic Link Issue ?)
if ((@HOST_PLATFORM = 'Windows') and  (CONVERT(INT,SERVERPROPERTY('ProductMajorVersion')) = 14)) 
  set @WWI_InMemory_Data_1 = STUFF(@DEFAULT_DATA_PATH,LEN(@DEFAULT_DATA_PATH)-4,4,'IN_MEMORY') + 'WideWorldImporters_InMemory_Data_1';
--
RESTORE DATABASE WorldWideImporters
FROM disk= '$(STAGE)/testdata/WideWorldImporters/WideWorldImporters-Full.bak'
WITH 
MOVE 'WWI_Primary'         TO @WWI_Primary,
MOVE 'WWI_UserData'        TO @WWI_UserData,
MOVE 'WWI_Log'             TO @WWI_Log,
MOVE 'WWI_InMemory_Data_1' TO @WWI_InMemory_Data_1,
REPLACE;
go

