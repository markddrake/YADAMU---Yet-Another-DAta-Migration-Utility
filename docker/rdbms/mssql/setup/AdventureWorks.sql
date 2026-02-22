drop database if exists AdventureWorks;
go
drop database if exists AdventureWorks$(MSSQL_VERSION);
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
DECLARE @Command  NVARCHAR(MAX) = N'RESTORE FILELISTONLY FROM DISK=''/var/opt/mssql/stage/testdata/AdventureWorks$(MSSQL_VERSION).bak''';
INSERT INTO ##FileList EXEC(@Command);
go
--
-- Step 2: Build dynamic restore command
DECLARE @AdventureWorksData NVARCHAR(4000) = CONVERT(NVARCHAR(4000),SERVERPROPERTY('InstanceDefaultDataPath')) + '/AdventureWorks.mdf';
DECLARE @AdventureWorksLog  NVARCHAR(4000) = CONVERT(NVARCHAR(4000),SERVERPROPERTY('InstanceDefaultDataPath')) + '/AdventureWorks.ldf';
--
DECLARE @DataFile NVARCHAR(128), @LogFile NVARCHAR(128);
DECLARE @DataPath NVARCHAR(4000) = CONVERT(NVARCHAR(4000),SERVERPROPERTY('InstanceDefaultDataPath')) + '/AdventureWorks.mdf';
DECLARE @LogPath NVARCHAR(4000) = CONVERT(NVARCHAR(4000),SERVERPROPERTY('InstanceDefaultDataPath')) + '/AdventureWorks.ldf';
DECLARE @SQL NVARCHAR(MAX);

SELECT @DataFile = LogicalName FROM ##FileList WHERE Type = 'D';
SELECT @LogFile = LogicalName FROM ##FileList WHERE Type = 'L';
SET @SQL = '
RESTORE DATABASE AdventureWorks$(MSSQL_VERSION)
FROM DISK=''/var/opt/mssql/stage/testdata/AdventureWorks$(MSSQL_VERSION).bak''
WITH 
MOVE ''' + @DataFile + ''' TO ''' + @DataPath + ''',
MOVE ''' + @LogFile + ''' TO ''' + @LogPath + ''',
REPLACE';

EXEC(@SQL);

ALTER DATABASE AdventureWorks$(MSSQL_VERSION) SET SINGLE_USER WITH ROLLBACK IMMEDIATE
GO
ALTER DATABASE AdventureWorks$(MSSQL_VERSION) MODIFY NAME = AdventureWorks
GO  
ALTER DATABASE AdventureWorks SET MULTI_USER
GO