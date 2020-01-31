RESTORE DATABASE WorldWideImportersDW
FROM disk= '/var/opt/mssql/backup/WideWorldImportersDW-Full.bak'
WITH MOVE 'WWI_Primary'
TO '/var/opt/mssql/data/WideWorldImportersDW.mdf',
MOVE 'WWI_UserData'
TO '/var/opt/mssql/data/WideWorldImporters_UserDataDW.mdf',
MOVE 'WWI_Log' 
TO '/var/opt/mssql/data/WideWorldImportersDW.ldf',
MOVE 'WWIDW_InMemory_Data_1' 
TO '/var/opt/mssql/data/WideWorldImportersDW_InMemory_Data_1',
REPLACE
