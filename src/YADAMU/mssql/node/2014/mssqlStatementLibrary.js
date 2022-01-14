"use strict" 

const DefaultStatmentLibrary = require('../mssqlStatementLibrary.js')

class MsSQLStatementLibrary extends DefaultStatmentLibrary {

  static get SQL_SYSTEM_INFORMATION()         { return _SQL_SYSTEM_INFORMATION }

  get SQL_SCHEMA_INFORMATION() {
     this._SQL_SCHEMA_INFORMATION = this._SQL_SCHEMA_INFORMATION || (() => { 
       const spatialClause = this.dbi.SPATIAL_MAKE_VALID === true ? `concat('case when "',c."COLUMN_NAME",'".STIsValid() = 0 then "',c."COLUMN_NAME",'".MakeValid().${this.dbi.SPATIAL_SERIALIZER} else "',c."COLUMN_NAME",'".${this.dbi.SPATIAL_SERIALIZER} end "',c."COLUMN_NAME",'"')` : `concat('"',c."COLUMN_NAME",'".${this.dbi.SPATIAL_SERIALIZER} "',c."COLUMN_NAME",'"')`
    
       return `select t."TABLE_SCHEMA" "TABLE_SCHEMA"
                     ,t."TABLE_NAME"   "TABLE_NAME"
                     ,concat(stuff((
                        select concat(',"',master.dbo.sp_JSON_ESCAPE(COLUMN_NAME),'"') as "data()"
                          from "INFORMATION_SCHEMA"."COLUMNS" c
                         where t."TABLE_CATALOG" = c."TABLE_CATALOG"
                           and t."TABLE_SCHEMA" = c."TABLE_SCHEMA"
                           and t."TABLE_NAME" = c."TABLE_NAME"
                         order by "ORDINAL_POSITION"
                           for XML PATH('')),1,1,'['),']') "COLUMN_NAME_ARRAY" 
                     ,concat(stuff((
                        select concat(',"',master.dbo.sp_JSON_ESCAPE(DATA_TYPE),'"') as "data()"
                         from "INFORMATION_SCHEMA"."COLUMNS" c
                        where t."TABLE_CATALOG" = c."TABLE_CATALOG"
                          and t."TABLE_SCHEMA" = c."TABLE_SCHEMA"
                          and t."TABLE_NAME" = c."TABLE_NAME"
                        order by "ORDINAL_POSITION"
                          for XML PATH('')),1,1,'['),']') "DATA_TYPE_ARRAY"                   
                     ,concat(stuff((
                        select concat(',"',master.dbo.sp_JSON_ESCAPE(COLLATION_NAME),'"') as "data()"
                          from "INFORMATION_SCHEMA"."COLUMNS" c
                         where t."TABLE_CATALOG" = c."TABLE_CATALOG"
                           and t."TABLE_SCHEMA" = c."TABLE_SCHEMA"
                           and t."TABLE_NAME" = c."TABLE_NAME"
                         order by "ORDINAL_POSITION"
                           for XML PATH('')),1,1,'['),']') "COLLATION_NAME_ARRAY"
                     ,concat(stuff((
                        select concat(',',
                                 case
                                   when ("NUMERIC_PRECISION" is not null) and ("NUMERIC_SCALE" is not null) then
                                     concat('"',"NUMERIC_PRECISION",',',"NUMERIC_SCALE",'"')
                                   when ("NUMERIC_PRECISION" is not null) then
                                     concat('"',"NUMERIC_PRECISION",'"')
                                   when ("DATETIME_PRECISION" is not null) then
                                     concat('"',"DATETIME_PRECISION",'"')
                                   when ("CHARACTER_MAXIMUM_LENGTH" is not null) then
                                     concat('"',"CHARACTER_MAXIMUM_LENGTH",'"')
                                   else
                                     '""'
                                 end
                               ) as "data()"
                          from "INFORMATION_SCHEMA"."COLUMNS" c
                         where t."TABLE_CATALOG" = c."TABLE_CATALOG"
                           and t."TABLE_SCHEMA" = c."TABLE_SCHEMA"
                           and t."TABLE_NAME" = c."TABLE_NAME"
                         order by "ORDINAL_POSITION"
                           for XML PATH('')),1,1,'['),']') "SIZE_CONSTRAINT_ARRAY"
                    ,stuff((
                        select concat(',',
                                 case 
                                   when "DATA_TYPE" = 'hierarchyid' then
                                     concat('cast("',c."COLUMN_NAME",'" as NVARCHAR(4000)) "',c."COLUMN_NAME",'"') 
                                   when "DATA_TYPE" in ('geometry','geography') then
                                     concat('"',c."COLUMN_NAME",'".AsBinaryZM() "',c."COLUMN_NAME",'"')
                                   when "DATA_TYPE" = 'datetime2' then
                                     concat('convert(VARCHAR(33),"',c."COLUMN_NAME",'",127) "',c."COLUMN_NAME",'"') 
                                   when "DATA_TYPE" = 'datetimeoffset' then
                                     concat('convert(VARCHAR(33),"',c."COLUMN_NAME",'",127) "',c."COLUMN_NAME",'"') 
                                                                     when "DATA_TYPE" = 'xml' then
                                    concat('convert(NVARCHAR(MAX),"',c."COLUMN_NAME",'") "',c."COLUMN_NAME",'"') 
                                  when "DATA_TYPE" in ('numeric','decimal') and ("NUMERIC_PRECISION" > 15) then
                                    -- Force Results to be returned as String
                                    case 
                                      when "NUMERIC_SCALE" > 0 then
                                        -- Retrieve as Text with Trailing Zeros: 
                                        -- WorldWideImportersDW.Fact.Order: Rows 231412. Reader Elapsed Time: 00:00:05.113s. 
                                        -- concat('concat('''',"',c."COLUMN_NAME",'") "',c."COLUMN_NAME",'"')
                                        -- Replace all zeros with spaces, remove trailing spaces and convert remaining spaces back to zeros.
                                        -- WorldWideImportersDW.Fact.Order: Rows 231412. Reader Elapsed Time: 00:00:05.064s. 
                                      concat('case when "',c."COLUMN_NAME",'" is NULL then NULL else replace(rtrim(replace("',c."COLUMN_NAME",'",''0'','' '')),'' '',''0'') end"',c."COLUMN_NAME",'"')
                                        -- Use SQL Format operator - Format is painfully slow
                                        -- WorldWideImportersDW.Fact.Order: Rows 231412. Reader Elapsed Time: 00:03:24.503s. 
                                        -- concat('format("',c."COLUMN_NAME",'",''g',"NUMERIC_PRECISION",''') "',c."COLUMN_NAME",'"') 
                                      else 
                                        concat('case when "',c."COLUMN_NAME",'" is NULL then NULL else concat('''',"',c."COLUMN_NAME",'") end "',c."COLUMN_NAME",'"')
                                    end 
                                  when "DATA_TYPE" in ('money') and ("NUMERIC_PRECISION" > 15) then
                                    concat('case when "',c."COLUMN_NAME",'" is NULL then NULL else replace(rtrim(replace(convert(VARCHAR,"',c."COLUMN_NAME",'",2),''0'','' '')),'' '',''0'') end"',c."COLUMN_NAME",'"')
                                  when "DATA_TYPE" in ('float','real') then
									-- concat('convert(VARCHAR,"',c."COLUMN_NAME",'",2) "',c."COLUMN_NAME",'"')
									concat('"',c."COLUMN_NAME",'"') 
                                  else 
                                    concat('"',c."COLUMN_NAME",'"') 
                                end
                              ) as "data()"
                         from "INFORMATION_SCHEMA"."COLUMNS" c
                        where t."TABLE_CATALOG" = c."TABLE_CATALOG"
                          and t."TABLE_SCHEMA" = c."TABLE_SCHEMA"
                          and t."TABLE_NAME" = c."TABLE_NAME"
                        order by "ORDINAL_POSITION"
                          for XML PATH('')),1,1,'') "CLIENT_SELECT_LIST"
              from "INFORMATION_SCHEMA"."TABLES" t
             where t."TABLE_TYPE" = 'BASE TABLE'
               and t."TABLE_SCHEMA" = @SCHEMA`;
    })();
    return this._SQL_SCHEMA_INFORMATION
  }     

  constructor(dbi) {
    super(dbi)
  }
}

module.exports = MsSQLStatementLibrary

const _SQL_SYSTEM_INFORMATION = 
`select db_Name() "DATABASE_NAME)", 
    current_user "CURRENT_USER)",
    session_user "SESSION_USER)", 
    concat ('{"BuildClrVersion": "',master.dbo.sp_JSON_ESCAPE(CAST(SERVERPROPERTY('BuildClrVersion') AS NVARCHAR)),'"'
       ,', "Collation": "',master.dbo.sp_JSON_ESCAPE(CAST(SERVERPROPERTY('Collation') AS NVARCHAR)),'"'
       ,', "CollationID": "',master.dbo.sp_JSON_ESCAPE(CAST(SERVERPROPERTY('CollationID') AS NVARCHAR)),'"'
       ,', "ComparisonStyle": "',master.dbo.sp_JSON_ESCAPE(CAST(SERVERPROPERTY('ComparisonStyle') AS NVARCHAR)),'"'
       ,', "ComputerNamePhysicalNetBIOS": "',master.dbo.sp_JSON_ESCAPE(CAST(SERVERPROPERTY('ComputerNamePhysicalNetBIOS') AS NVARCHAR)),'"'
       ,', "Edition": "',master.dbo.sp_JSON_ESCAPE(CAST(SERVERPROPERTY('Edition') AS NVARCHAR)),'"'
       ,', "EditionID": "',master.dbo.sp_JSON_ESCAPE(CAST(SERVERPROPERTY('EditionID') AS NVARCHAR)),'"'
       ,', "EngineEdition": "',master.dbo.sp_JSON_ESCAPE(CAST(SERVERPROPERTY('EngineEdition') AS NVARCHAR)),'"'
       ,', "HadrManagerStatus": "',master.dbo.sp_JSON_ESCAPE(CAST(SERVERPROPERTY('HadrManagerStatus') AS NVARCHAR)),'"'
       ,', "InstanceDefaultDataPath": "',master.dbo.sp_JSON_ESCAPE(CAST(SERVERPROPERTY('InstanceDefaultDataPath') AS NVARCHAR)),'"'
       ,', "InstanceDefaultLogPath": "',master.dbo.sp_JSON_ESCAPE(CAST(SERVERPROPERTY('InstanceDefaultLogPath') AS NVARCHAR)),'"'
       ,', "InstanceName": "',master.dbo.sp_JSON_ESCAPE(CAST(SERVERPROPERTY('InstanceName') AS NVARCHAR)),'"'
       ,', "IsAdvancedAnalyticsInstalled": "',master.dbo.sp_JSON_ESCAPE(CAST(SERVERPROPERTY('IsAdvancedAnalyticsInstalled') AS NVARCHAR)),'"'
       ,', "IsBigDataCluster": "',master.dbo.sp_JSON_ESCAPE(CAST(SERVERPROPERTY('IsBigDataCluster') AS NVARCHAR)),'"'
       ,', "IsClustered": "',master.dbo.sp_JSON_ESCAPE(CAST(SERVERPROPERTY('IsClustered') AS NVARCHAR)),'"'
       ,', "IsFullTextInstalled": "',master.dbo.sp_JSON_ESCAPE(CAST(SERVERPROPERTY('IsFullTextInstalled') AS NVARCHAR)),'"'
       ,', "IsHadrEnabled": "',master.dbo.sp_JSON_ESCAPE(CAST(SERVERPROPERTY('IsHadrEnabled') AS NVARCHAR)),'"'
       ,', "IsIntegratedSecurityOnly": "',master.dbo.sp_JSON_ESCAPE(CAST(SERVERPROPERTY('IsIntegratedSecurityOnly') AS NVARCHAR)),'"'
       ,', "IsLocalDB": "',master.dbo.sp_JSON_ESCAPE(CAST(SERVERPROPERTY('IsLocalDB') AS NVARCHAR)),'"'
       ,', "IsPolyBaseInstalled": "',master.dbo.sp_JSON_ESCAPE(CAST(SERVERPROPERTY('IsPolyBaseInstalled') AS NVARCHAR)),'"'
       ,', "IsSingleUser": "',master.dbo.sp_JSON_ESCAPE(CAST(SERVERPROPERTY('IsSingleUser') AS NVARCHAR)),'"'
       ,', "IsXTPSupported": "',master.dbo.sp_JSON_ESCAPE(CAST(SERVERPROPERTY('IsXTPSupported') AS NVARCHAR)),'"'
       ,', "LCID": "',master.dbo.sp_JSON_ESCAPE(CAST(SERVERPROPERTY('LCID') AS NVARCHAR)),'"'
       ,', "LicenseType": "',master.dbo.sp_JSON_ESCAPE(CAST(SERVERPROPERTY('LicenseType') AS NVARCHAR)),'"'
       ,', "MachineName": "',master.dbo.sp_JSON_ESCAPE(CAST(SERVERPROPERTY('MachineName') AS NVARCHAR)),'"'
       ,', "NumLicenses": "',master.dbo.sp_JSON_ESCAPE(CAST(SERVERPROPERTY('NumLicenses') AS NVARCHAR)),'"'
       ,', "ProcessID": "',master.dbo.sp_JSON_ESCAPE(CAST(SERVERPROPERTY('ProcessID') AS NVARCHAR)),'"'
       ,', "ProductBuild": "',master.dbo.sp_JSON_ESCAPE(CAST(SERVERPROPERTY('ProductBuild') AS NVARCHAR)),'"'
       ,', "ProductBuildType": "',master.dbo.sp_JSON_ESCAPE(CAST(SERVERPROPERTY('ProductBuildType') AS NVARCHAR)),'"'
       ,', "ProductLevel": "',master.dbo.sp_JSON_ESCAPE(CAST(SERVERPROPERTY('ProductLevel') AS NVARCHAR)),'"'
       ,', "ProductMajorVersion": "',master.dbo.sp_JSON_ESCAPE(CAST(SERVERPROPERTY('ProductMajorVersion') AS NVARCHAR)),'"'
       ,', "ProductMinorVersion": "',master.dbo.sp_JSON_ESCAPE(CAST(SERVERPROPERTY('ProductMinorVersion') AS NVARCHAR)),'"'
       ,', "ProductUpdateLevel": "',master.dbo.sp_JSON_ESCAPE(CAST(SERVERPROPERTY('ProductUpdateLevel') AS NVARCHAR)),'"'
       ,', "ProductUpdateReference": "',master.dbo.sp_JSON_ESCAPE(CAST(SERVERPROPERTY('ProductUpdateReference') AS NVARCHAR)),'"'
       ,', "ProductVersion": "',master.dbo.sp_JSON_ESCAPE(CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR)),'"'
       ,', "ResourceLastUpdateDateTime": "',master.dbo.sp_JSON_ESCAPE(CAST(SERVERPROPERTY('ResourceLastUpdateDateTime') AS NVARCHAR)),'"'
       ,', "ResourceVersion": "',master.dbo.sp_JSON_ESCAPE(CAST(SERVERPROPERTY('ResourceVersion') AS NVARCHAR)),'"'
       ,', "ServerName": "',master.dbo.sp_JSON_ESCAPE(CAST(SERVERPROPERTY('ServerName') AS NVARCHAR)),'"'
       ,', "SqlCharSet": "',master.dbo.sp_JSON_ESCAPE(CAST(SERVERPROPERTY('SqlCharSet') AS NVARCHAR)),'"'
       ,', "SqlCharSetName": "',master.dbo.sp_JSON_ESCAPE(CAST(SERVERPROPERTY('SqlCharSetName') AS NVARCHAR)),'"'
       ,', "SqlSortOrder": "',master.dbo.sp_JSON_ESCAPE(CAST(SERVERPROPERTY('SqlSortOrder') AS NVARCHAR)),'"'
       ,', "SqlSortOrderName": "',master.dbo.sp_JSON_ESCAPE(CAST(SERVERPROPERTY('SqlSortOrderName') AS NVARCHAR)),'"'
       ,', "FilestreamShareName": "',master.dbo.sp_JSON_ESCAPE(CAST(SERVERPROPERTY('FilestreamShareName') AS NVARCHAR)),'"'
       ,', "FilestreamConfiguredLevel": "',master.dbo.sp_JSON_ESCAPE(CAST(SERVERPROPERTY('FilestreamConfiguredLevel') AS NVARCHAR)),'"'
       ,', "FilestreamEffectiveLevel": "',master.dbo.sp_JSON_ESCAPE(CAST(SERVERPROPERTY('FilestreamEffectiveLevel') AS NVARCHAR)),'"}'
    ) "SERVER_PROPERTIES"
    ,concat(
        '{"Collation": "',master.dbo.sp_JSON_ESCAPE(CAST(DATABASEPROPERTYEX(DB_NAME(),'Collation') AS NVARCHAR)),'"'
       ,', "ComparisonStyle": "',master.dbo.sp_JSON_ESCAPE(CAST(DATABASEPROPERTYEX(DB_NAME(),'ComparisonStyle') AS NVARCHAR)),'"'
       ,', "Edition": "',master.dbo.sp_JSON_ESCAPE(CAST(DATABASEPROPERTYEX(DB_NAME(),'Edition') AS NVARCHAR)),'"'
       ,', "IsAnsiNullDefault": "',master.dbo.sp_JSON_ESCAPE(CAST(DATABASEPROPERTYEX(DB_NAME(),'IsAnsiNullDefault') AS NVARCHAR)),'"'
       ,', "IsAnsiNullsEnabled": "',master.dbo.sp_JSON_ESCAPE(CAST(DATABASEPROPERTYEX(DB_NAME(),'IsAnsiNullsEnabled') AS NVARCHAR)),'"'
       ,', "IsAnsiPaddingEnabled": "',master.dbo.sp_JSON_ESCAPE(CAST(DATABASEPROPERTYEX(DB_NAME(),'IsAnsiPaddingEnabled') AS NVARCHAR)),'"'
       ,', "IsAnsiWarningsEnabled": "',master.dbo.sp_JSON_ESCAPE(CAST(DATABASEPROPERTYEX(DB_NAME(),'IsAnsiWarningsEnabled') AS NVARCHAR)),'"'
       ,', "IsArithmeticAbortEnabled": "',master.dbo.sp_JSON_ESCAPE(CAST(DATABASEPROPERTYEX(DB_NAME(),'IsArithmeticAbortEnabled') AS NVARCHAR)),'"'
       ,', "IsAutoClose": "',master.dbo.sp_JSON_ESCAPE(CAST(DATABASEPROPERTYEX(DB_NAME(),'IsAutoClose') AS NVARCHAR)),'"'
       ,', "IsAutoCreateStatistics": "',master.dbo.sp_JSON_ESCAPE(CAST(DATABASEPROPERTYEX(DB_NAME(),'IsAutoCreateStatistics') AS NVARCHAR)),'"'
       ,', "IsAutoCreateStatisticsIncremental": "',master.dbo.sp_JSON_ESCAPE(CAST(DATABASEPROPERTYEX(DB_NAME(),'IsAutoCreateStatisticsIncremental') AS NVARCHAR)),'"'
       ,', "IsAutoShrink": "',master.dbo.sp_JSON_ESCAPE(CAST(DATABASEPROPERTYEX(DB_NAME(),'IsAutoShrink') AS NVARCHAR)),'"'
       ,', "IsAutoUpdateStatistics": "',master.dbo.sp_JSON_ESCAPE(CAST(DATABASEPROPERTYEX(DB_NAME(),'IsAutoUpdateStatistics') AS NVARCHAR)),'"'
       ,', "IsClone": "',master.dbo.sp_JSON_ESCAPE(CAST(DATABASEPROPERTYEX(DB_NAME(),'IsClone') AS NVARCHAR)),'"'
       ,', "IsCloseCursorsOnCommitEnabled": "',master.dbo.sp_JSON_ESCAPE(CAST(DATABASEPROPERTYEX(DB_NAME(),'IsCloseCursorsOnCommitEnabled') AS NVARCHAR)),'"'
       ,', "IsFulltextEnabled": "',master.dbo.sp_JSON_ESCAPE(CAST(DATABASEPROPERTYEX(DB_NAME(),'IsFulltextEnabled') AS NVARCHAR)),'"'
       ,', "IsInStandBy": "',master.dbo.sp_JSON_ESCAPE(CAST(DATABASEPROPERTYEX(DB_NAME(),'IsInStandBy') AS NVARCHAR)),'"'
       ,', "IsLocalCursorsDefault": "',master.dbo.sp_JSON_ESCAPE(CAST(DATABASEPROPERTYEX(DB_NAME(),'IsLocalCursorsDefault') AS NVARCHAR)),'"'
       ,', "IsMemoryOptimizedElevateToSnapshotEnabled": "',master.dbo.sp_JSON_ESCAPE(CAST(DATABASEPROPERTYEX(DB_NAME(),'IsMemoryOptimizedElevateToSnapshotEnabled') AS NVARCHAR)),'"'
       ,', "IsMergePublished": "',master.dbo.sp_JSON_ESCAPE(CAST(DATABASEPROPERTYEX(DB_NAME(),'IsMergePublished') AS NVARCHAR)),'"'
       ,', "IsNullConcat": "',master.dbo.sp_JSON_ESCAPE(CAST(DATABASEPROPERTYEX(DB_NAME(),'IsNullConcat') AS NVARCHAR)),'"'
       ,', "IsNumericRoundAbortEnabled": "',master.dbo.sp_JSON_ESCAPE(CAST(DATABASEPROPERTYEX(DB_NAME(),'IsNumericRoundAbortEnabled') AS NVARCHAR)),'"'
       ,', "IsParameterizationForced": "',master.dbo.sp_JSON_ESCAPE(CAST(DATABASEPROPERTYEX(DB_NAME(),'IsParameterizationForced') AS NVARCHAR)),'"'
       ,', "IsQuotedIdentifiersEnabled": "',master.dbo.sp_JSON_ESCAPE(CAST(DATABASEPROPERTYEX(DB_NAME(),'IsQuotedIdentifiersEnabled') AS NVARCHAR)),'"'
       ,', "IsPublished": "',master.dbo.sp_JSON_ESCAPE(CAST(DATABASEPROPERTYEX(DB_NAME(),'IsPublished') AS NVARCHAR)),'"'
       ,', "IsRecursiveTriggersEnabled": "',master.dbo.sp_JSON_ESCAPE(CAST(DATABASEPROPERTYEX(DB_NAME(),'IsRecursiveTriggersEnabled') AS NVARCHAR)),'"'
       ,', "IsSubscribed": "',master.dbo.sp_JSON_ESCAPE(CAST(DATABASEPROPERTYEX(DB_NAME(),'IsSubscribed') AS NVARCHAR)),'"'
       ,', "IsSyncWithBackup": "',master.dbo.sp_JSON_ESCAPE(CAST(DATABASEPROPERTYEX(DB_NAME(),'IsSyncWithBackup') AS NVARCHAR)),'"'
       ,', "IsTornPageDetectionEnabled": "',master.dbo.sp_JSON_ESCAPE(CAST(DATABASEPROPERTYEX(DB_NAME(),'IsTornPageDetectionEnabled') AS NVARCHAR)),'"'
       ,', "IsVerifiedClone": "',master.dbo.sp_JSON_ESCAPE(CAST(DATABASEPROPERTYEX(DB_NAME(),'IsVerifiedClone') AS NVARCHAR)),'"'
       ,', "IsXTPSupported": "',master.dbo.sp_JSON_ESCAPE(CAST(DATABASEPROPERTYEX(DB_NAME(),'IsXTPSupported') AS NVARCHAR)),'"'
       ,', "LastGoodCheckDbTime": "',master.dbo.sp_JSON_ESCAPE(CAST(DATABASEPROPERTYEX(DB_NAME(),'LastGoodCheckDbTime') AS NVARCHAR)),'"'
       ,', "LCID": "',master.dbo.sp_JSON_ESCAPE(CAST(DATABASEPROPERTYEX(DB_NAME(),'LCID') AS NVARCHAR)),'"'
       ,', "MaxSizeInBytes": "',master.dbo.sp_JSON_ESCAPE(CAST(DATABASEPROPERTYEX(DB_NAME(),'MaxSizeInBytes') AS NVARCHAR)),'"'
       ,', "Recovery": "',master.dbo.sp_JSON_ESCAPE(CAST(DATABASEPROPERTYEX(DB_NAME(),'Recovery') AS NVARCHAR)),'"'
       ,', "ServiceObjective": "',master.dbo.sp_JSON_ESCAPE(CAST(DATABASEPROPERTYEX(DB_NAME(),'ServiceObjective') AS NVARCHAR)),'"'
       ,', "ServiceObjectiveId": "',master.dbo.sp_JSON_ESCAPE(CAST(DATABASEPROPERTYEX(DB_NAME(),'ServiceObjectiveId') AS NVARCHAR)),'"'
       ,', "SQLSortOrder": "',master.dbo.sp_JSON_ESCAPE(CAST(DATABASEPROPERTYEX(DB_NAME(),'SQLSortOrder') AS NVARCHAR)),'"'
       ,', "Status": "',master.dbo.sp_JSON_ESCAPE(CAST(DATABASEPROPERTYEX(DB_NAME(),'Status') AS NVARCHAR)),'"'
       ,', "Updateability": "',master.dbo.sp_JSON_ESCAPE(CAST(DATABASEPROPERTYEX(DB_NAME(),'Updateability') AS NVARCHAR)),'"'
       ,', "UserAccess": "',master.dbo.sp_JSON_ESCAPE(CAST(DATABASEPROPERTYEX(DB_NAME(),'UserAccess') AS NVARCHAR)),'"'
       ,', "Version": "',master.dbo.sp_JSON_ESCAPE(CAST(DATABASEPROPERTYEX(DB_NAME(),'Version') AS NVARCHAR)),'"}'
     ) "DATABASE_PROPERTIES",
		master.dbo.sp_YADAMU_INSTANCE_ID() "YADAMU_INSTANCE_ID",
		master.dbo.sp_YADAMU_INSTALLATION_TIMESTAMP() "YADAMU_INSTALLATION_TIMESTAMP"`;


