RESTORE DATABASE WorldWideImporters
FROM disk= '/var/opt/mssql/backup/WideWorldImporters-Full.bak'
WITH MOVE 'WWI_Primary'
TO '/var/opt/mssql/data/WideWorldImporters.mdf',
MOVE 'WWI_UserData'
TO '/var/opt/mssql/data/WideWorldImporters_UserData.mdf',
MOVE 'WWI_Log' 
TO '/var/opt/mssql/data/WideWorldImporters.ldf',
MOVE 'WWI_InMemory_Data_1' 
TO '/var/opt/mssql/data/WideWorldImporters_InMemory_Data_1',
REPLACE
