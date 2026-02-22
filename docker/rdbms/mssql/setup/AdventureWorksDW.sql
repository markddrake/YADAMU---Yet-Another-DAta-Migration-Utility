drop database if exists AdventureWorksDW;
go
drop database if exists AdventureWorksDW$(MSSQL_VERSION);
go
--
CREATE TABLE ##FileList (
  LogicalName nvarchar(128)
, PhysicalName nvarchar(260)
, [Type] char(1)
, FileGroupName nvarchar(128)
, Size numeric(20,0)
, MaxSize numeric(20,0)
, FileID bigint
, CreateLSN numeric(25,0)
, DropLSN numeric(25,0)
, UniqueID uniqueidentifier
, ReadOnlyLSN numeric(25,0)
, ReadWriteLSN numeric(25,0)
, BackupSizeInBytes bigint
, SourceBlockSize int
, FileGroupId int
, LogGroupGUID uniqueidentifier
, DifferentialBaseLSN numeric(25,0)
, DifferentialBaseGUID uniqueidentifier
, IsReadOnly bit
, IsPresent bit
, TDEThumbprint varbinary(32)
, SnapshotURL nvarchar(360) 
);
GO
--
DECLARE @Command  NVARCHAR(MAX) = N'RESTORE FILELISTONLY FROM DISK=''/var/opt/mssql/stage/testdata/AdventureWorksDW$(MSSQL_VERSION).bak''';
INSERT INTO ##FileList EXEC(@Command);
go
--
DECLARE @AdventureWorksDW       NVARCHAR(4000) = CONVERT(NVARCHAR(4000),SERVERPROPERTY('InstanceDefaultDataPath')) + '/AdventureWorksDW.mdf';
DECLARE @AdventureWorksDW_Log   NVARCHAR(4000) = CONVERT(NVARCHAR(4000),SERVERPROPERTY('InstanceDefaultDataPath')) + '/AdventureWorksDW.ldf';
--
DECLARE @DataFile NVARCHAR(128), @LogFile NVARCHAR(128);
DECLARE @DataPath NVARCHAR(4000) = CONVERT(NVARCHAR(4000),SERVERPROPERTY('InstanceDefaultDataPath')) + '/AdventureWorksDW.mdf';
DECLARE @LogPath NVARCHAR(4000) = CONVERT(NVARCHAR(4000),SERVERPROPERTY('InstanceDefaultDataPath')) + '/AdventureWorksDW.ldf';
DECLARE @SQL NVARCHAR(MAX);
--
SELECT @DataFile = LogicalName FROM ##FileList WHERE Type = 'D';
SELECT @LogFile = LogicalName FROM ##FileList WHERE Type = 'L';
SET @SQL = '
RESTORE DATABASE AdventureWorksDW$(MSSQL_VERSION)
FROM DISK=''/var/opt/mssql/stage/testdata/AdventureWorksDW$(MSSQL_VERSION).bak''
WITH 
MOVE ''' + @DataFile + ''' TO ''' + @DataPath + ''',
MOVE ''' + @LogFile + ''' TO ''' + @LogPath + ''',
REPLACE';
EXEC(@SQL);
go
--
ALTER DATABASE AdventureWorksDW$(MSSQL_VERSION) SET SINGLE_USER WITH ROLLBACK IMMEDIATE
GO
ALTER DATABASE AdventureWorksDW$(MSSQL_VERSION) MODIFY NAME = AdventureWorksDW
GO  
ALTER DATABASE AdventureWorksDW SET MULTI_USER
GO

