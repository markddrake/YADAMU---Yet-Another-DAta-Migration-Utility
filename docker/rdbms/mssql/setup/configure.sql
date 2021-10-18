-- Enable advanced options
exec sp_configure 'show advanced option', '1'; 
reconfigure;
go
-- Max Memory 16G
exec sys.sp_configure 'max server memory (MB)', '$(MSSQL_MEMORY)'; 
reconfigure;
go
-- Disable advanced options
exec sp_configure 'show advanced option', '0';
reconfigure;
go
