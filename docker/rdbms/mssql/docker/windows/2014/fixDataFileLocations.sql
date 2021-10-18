use MASTER
go
ALTER LOGIN sa
WITH PASSWORD = 'oracle#1'
go
ALTER DATABASE model
MODIFY FILE (
  NAME = modeldev, 
  FILENAME = 'C:\Program Files\Microsoft SQL Server\MSSQL12.MSSQLSERVER\MSSQL\DATA\model.mdf'
);
go
ALTER DATABASE model
MODIFY FILE (
  NAME = modellog, 
  FILENAME = 'C:\Program Files\Microsoft SQL Server\MSSQL12.MSSQLSERVER\MSSQL\DATA\\modellog.ldf'
);
go
ALTER DATABASE tempdb 
MODIFY FILE (
   NAME = tempdev, 
   FILENAME = 'C:\Program Files\Microsoft SQL Server\MSSQL12.MSSQLSERVER\MSSQL\DATA\tempdb.mdf'
);
go
ALTER DATABASE tempdb
MODIFY FILE (
  NAME = templog, 
  FILENAME = 'C:\Program Files\Microsoft SQL Server\MSSQL12.MSSQLSERVER\MSSQL\DATA\templog.ldf'
);
go
ALTER DATABASE msdb
MODIFY FILE (
  NAME = MSDBData,
  FILENAME = 'C:\Program Files\Microsoft SQL Server\MSSQL12.MSSQLSERVER\MSSQL\DATA\MSDBData.mdf'
);
ALTER DATABASE msdb
MODIFY FILE (
  NAME = MSDBLog,
  FILENAME = 'C:\Program Files\Microsoft SQL Server\MSSQL12.MSSQLSERVER\MSSQL\DATA\MSDBLog.ld'
);
go
