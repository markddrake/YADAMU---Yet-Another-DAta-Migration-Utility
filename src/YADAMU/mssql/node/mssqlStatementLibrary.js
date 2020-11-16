"use strict" 

const YadamuConstants = require('../../common/yadamuConstants.js');

class MsSQLStatementLibrary {

  static get SQL_SYSTEM_INFORMATION()         { return _SQL_SYSTEM_INFORMATION }
  static get SQL_CREATE_SAVE_POINT()          { return _SQL_CREATE_SAVE_POINT }  
  static get SQL_RESTORE_SAVE_POINT()         { return _SQL_RESTORE_SAVE_POINT }

  get SQL_SCHEMA_INFORMATION() {
     this._SQL_SCHEMA_INFORMATION = this._SQL_SCHEMA_INFORMATION || (() => { 
       const spatialClause = this.dbi.SPATIAL_MAKE_VALID === true ? `concat('case when "',c."COLUMN_NAME",'".STIsValid() = 0 then "',c."COLUMN_NAME",'".MakeValid().${this.dbi.SPATIAL_SERIALIZER} else "',c."COLUMN_NAME",'".${this.dbi.SPATIAL_SERIALIZER} end "',c."COLUMN_NAME",'"')` : `concat('"',c."COLUMN_NAME",'".${this.dbi.SPATIAL_SERIALIZER} "',c."COLUMN_NAME",'"')`
    
       return `select t."TABLE_SCHEMA" "TABLE_SCHEMA"
                     ,t."TABLE_NAME"   "TABLE_NAME"
                     ,concat('[',string_agg(concat('"',c."COLUMN_NAME",'"'),',') within group (order by "ORDINAL_POSITION"),']') "COLUMN_NAME_ARRAY"
                     ,concat('[',string_agg(case
                                            when cc."CONSTRAINT_NAME" is not NULL then 
                                              '"JSON"'
                                            else 
                                              concat('"',"DATA_TYPE",'"')
                                            end
                                          ,',') within group (order by "ORDINAL_POSITION"),']') "DATA_TYPE_ARRAY"
                   ,concat('[',string_agg(concat('"',"COLLATION_NAME",'"'),',') within group (order by "ORDINAL_POSITION"),']') "COLLATION_NAME_ARRAY"
                   ,concat('[',string_agg(case
                                  when ("NUMERIC_PRECISION" is not null) and ("NUMERIC_SCALE" is not null) 
                                    then concat('"',"NUMERIC_PRECISION",',',"NUMERIC_SCALE",'"')
                                  when ("NUMERIC_PRECISION" is not null) 
                                    then concat('"',"NUMERIC_PRECISION",'"')
                                  when ("DATETIME_PRECISION" is not null)
                                    then concat('"',"DATETIME_PRECISION",'"')
                                  when ("CHARACTER_MAXIMUM_LENGTH" is not null)
                                    then concat('"',"CHARACTER_MAXIMUM_LENGTH",'"')
                                  else
                                    '""'
                                end
                               ,','
                              )
                     within group (order by "ORDINAL_POSITION"),']') "SIZE_CONSTRAINT_ARRAY"
                    ,string_agg(case 
                                                   when "DATA_TYPE" = 'hierarchyid' then
                                                     concat('cast("',c."COLUMN_NAME",'" as NVARCHAR(4000)) "',c."COLUMN_NAME",'"') 
                                                   when "DATA_TYPE" in ('geometry','geography') then
                                                     ${spatialClause}
                                                   when "DATA_TYPE" = 'datetime2' then
                                                     concat('convert(VARCHAR(33),"',c."COLUMN_NAME",'",127) "',c."COLUMN_NAME",'"') 
                                                   when "DATA_TYPE" = 'datetimeoffset' then
                                                     concat('convert(VARCHAR(33),"',c."COLUMN_NAME",'",127) "',c."COLUMN_NAME",'"') 
                                                   when "DATA_TYPE" = 'xml' then
                                                     concat('replace(replace(convert(NVARCHAR(MAX),"',c."COLUMN_NAME",'"),''&#x0A;'',''\n''),''&#x20;'','' '') "',c."COLUMN_NAME",'"') 
                                                   else 
                                                     concat('"',c."COLUMN_NAME",'"') 
                                                 end
                                                ,','
                                                ) 
                               within group (order by "ORDINAL_POSITION"
                              ) "CLIENT_SELECT_LIST"
               from "INFORMATION_SCHEMA"."COLUMNS" c
                    left join "INFORMATION_SCHEMA"."TABLES" t
                        on t."TABLE_CATALOG" = c."TABLE_CATALOG"
                       and t."TABLE_SCHEMA" = c."TABLE_SCHEMA"
                       and t."TABLE_NAME" = c."TABLE_NAME"
                     left outer join (
                        "INFORMATION_SCHEMA"."CONSTRAINT_COLUMN_USAGE" ccu
                        left join "INFORMATION_SCHEMA"."CHECK_CONSTRAINTS" cc
                          on cc."CONSTRAINT_CATALOG" = ccu."CONSTRAINT_CATALOG"
                         and cc."CONSTRAINT_SCHEMA" = ccu."CONSTRAINT_SCHEMA"
                         and cc."CONSTRAINT_NAME" = ccu."CONSTRAINT_NAME"
                        )
                        on ccu."TABLE_CATALOG" = c."TABLE_CATALOG"
                       and ccu."TABLE_SCHEMA" = c."TABLE_SCHEMA"
                       and ccu."TABLE_NAME" = c."TABLE_NAME"
                       and ccu."COLUMN_NAME" = c."COLUMN_NAME"
                       and UPPER("CHECK_CLAUSE") like '(ISJSON(%)>(0))'
              where t."TABLE_TYPE" = 'BASE TABLE'
                and t."TABLE_SCHEMA" = @SCHEMA
              group by t."TABLE_SCHEMA", t."TABLE_NAME"`;  
	})();
	return this._SQL_SCHEMA_INFORMATION
  }     
  
  get DROP_DATABASE() { return `drop database if exists "${this.dbi.parameters.YADAMU_DATABASE}"` }
  
  constructor(dbi) {
    this.dbi = dbi
  }
  
}

module.exports = MsSQLStatementLibrary

const _SQL_SYSTEM_INFORMATION = 
`select db_Name() "DATABASE_NAME", 
       current_user "CURRENT_USER", 
       session_user "SESSION_USER", 
       (
         select
           SERVERPROPERTY('BuildClrVersion') AS "BuildClrVersion"
          ,SERVERPROPERTY('Collation') AS "Collation"
          ,SERVERPROPERTY('CollationID') AS "CollationID"
          ,SERVERPROPERTY('ComparisonStyle') AS "ComparisonStyle"
          ,SERVERPROPERTY('ComputerNamePhysicalNetBIOS') AS "ComputerNamePhysicalNetBIOS"
          ,SERVERPROPERTY('Edition') AS "Edition"
          ,SERVERPROPERTY('EditionID') AS "EditionID"
          ,SERVERPROPERTY('EngineEdition') AS "EngineEdition"
          ,SERVERPROPERTY('HadrManagerStatus') AS "HadrManagerStatus"
          ,SERVERPROPERTY('InstanceDefaultDataPath') AS "InstanceDefaultDataPath"
          ,SERVERPROPERTY('InstanceDefaultLogPath') AS "InstanceDefaultLogPath"
          ,SERVERPROPERTY('InstanceName') AS "InstanceName"
          ,SERVERPROPERTY('IsAdvancedAnalyticsInstalled') AS "IsAdvancedAnalyticsInstalled"
          ,SERVERPROPERTY('IsBigDataCluster') AS "IsBigDataCluster"
          ,SERVERPROPERTY('IsClustered') AS "IsClustered"
          ,SERVERPROPERTY('IsFullTextInstalled') AS "IsFullTextInstalled"
          ,SERVERPROPERTY('IsHadrEnabled') AS "IsHadrEnabled"
          ,SERVERPROPERTY('IsIntegratedSecurityOnly') AS "IsIntegratedSecurityOnly"
          ,SERVERPROPERTY('IsLocalDB') AS "IsLocalDB"
          ,SERVERPROPERTY('IsPolyBaseInstalled') AS "IsPolyBaseInstalled"
          ,SERVERPROPERTY('IsSingleUser') AS "IsSingleUser"
          ,SERVERPROPERTY('IsXTPSupported') AS "IsXTPSupported"
          ,SERVERPROPERTY('LCID') AS "LCID"
          ,SERVERPROPERTY('LicenseType') AS "LicenseType"
          ,SERVERPROPERTY('MachineName') AS "MachineName"
          ,SERVERPROPERTY('NumLicenses') AS "NumLicenses"
          ,SERVERPROPERTY('ProcessID') AS "ProcessID"
          ,SERVERPROPERTY('ProductBuild') AS "ProductBuild"
          ,SERVERPROPERTY('ProductBuildType') AS "ProductBuildType"
          ,SERVERPROPERTY('ProductLevel') AS "ProductLevel"
          ,SERVERPROPERTY('ProductMajorVersion') AS "ProductMajorVersion"
          ,SERVERPROPERTY('ProductMinorVersion') AS "ProductMinorVersion"
          ,SERVERPROPERTY('ProductUpdateLevel') AS "ProductUpdateLevel"
          ,SERVERPROPERTY('ProductUpdateReference') AS "ProductUpdateReference"
          ,SERVERPROPERTY('ProductVersion') AS "ProductVersion"
          ,SERVERPROPERTY('ResourceLastUpdateDateTime') AS "ResourceLastUpdateDateTime"
          ,SERVERPROPERTY('ResourceVersion') AS "ResourceVersion"
          ,SERVERPROPERTY('ServerName') AS "ServerName"
          ,SERVERPROPERTY('SqlCharSet') AS "SqlCharSet"
          ,SERVERPROPERTY('SqlCharSetName') AS "SqlCharSetName"
          ,SERVERPROPERTY('SqlSortOrder') AS "SqlSortOrder"
          ,SERVERPROPERTY('SqlSortOrderName') AS "SqlSortOrderName"
          ,SERVERPROPERTY('FilestreamShareName') AS "FilestreamShareName"
          ,SERVERPROPERTY('FilestreamConfiguredLevel') AS "FilestreamConfiguredLevel"
          ,SERVERPROPERTY('FilestreamEffectiveLevel') AS "FilestreamEffectiveLevel"
         FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
       ) "SERVER_PROPERTIES",
       (
         select
           DATABASEPROPERTYEX(DB_NAME(),'Collation') AS "Collation"
          ,DATABASEPROPERTYEX(DB_NAME(),'ComparisonStyle') AS "ComparisonStyle"
          ,DATABASEPROPERTYEX(DB_NAME(),'Edition') AS "Edition"
          ,DATABASEPROPERTYEX(DB_NAME(),'IsAnsiNullDefault') AS "IsAnsiNullDefault"
          ,DATABASEPROPERTYEX(DB_NAME(),'IsAnsiNullsEnabled') AS "IsAnsiNullsEnabled"
          ,DATABASEPROPERTYEX(DB_NAME(),'IsAnsiPaddingEnabled') AS "IsAnsiPaddingEnabled"
          ,DATABASEPROPERTYEX(DB_NAME(),'IsAnsiWarningsEnabled') AS "IsAnsiWarningsEnabled"
          ,DATABASEPROPERTYEX(DB_NAME(),'IsArithmeticAbortEnabled') AS "IsArithmeticAbortEnabled"
          ,DATABASEPROPERTYEX(DB_NAME(),'IsAutoClose') AS "IsAutoClose"
          ,DATABASEPROPERTYEX(DB_NAME(),'IsAutoCreateStatistics') AS "IsAutoCreateStatistics"
          ,DATABASEPROPERTYEX(DB_NAME(),'IsAutoCreateStatisticsIncremental') AS "IsAutoCreateStatisticsIncremental"
          ,DATABASEPROPERTYEX(DB_NAME(),'IsAutoShrink') AS "IsAutoShrink"
          ,DATABASEPROPERTYEX(DB_NAME(),'IsAutoUpdateStatistics') AS "IsAutoUpdateStatistics"
          ,DATABASEPROPERTYEX(DB_NAME(),'IsClone') AS "IsClone"
          ,DATABASEPROPERTYEX(DB_NAME(),'IsCloseCursorsOnCommitEnabled') AS "IsCloseCursorsOnCommitEnabled"
          ,DATABASEPROPERTYEX(DB_NAME(),'IsFulltextEnabled') AS "IsFulltextEnabled"
          ,DATABASEPROPERTYEX(DB_NAME(),'IsInStandBy') AS "IsInStandBy"
          ,DATABASEPROPERTYEX(DB_NAME(),'IsLocalCursorsDefault') AS "IsLocalCursorsDefault"
          ,DATABASEPROPERTYEX(DB_NAME(),'IsMemoryOptimizedElevateToSnapshotEnabled') AS "IsMemoryOptimizedElevateToSnapshotEnabled"
          ,DATABASEPROPERTYEX(DB_NAME(),'IsMergePublished') AS "IsMergePublished"
          ,DATABASEPROPERTYEX(DB_NAME(),'IsNullConcat') AS "IsNullConcat"
          ,DATABASEPROPERTYEX(DB_NAME(),'IsNumericRoundAbortEnabled') AS "IsNumericRoundAbortEnabled"
          ,DATABASEPROPERTYEX(DB_NAME(),'IsParameterizationForced') AS "IsParameterizationForced"
          ,DATABASEPROPERTYEX(DB_NAME(),'IsQuotedIdentifiersEnabled') AS "IsQuotedIdentifiersEnabled"
          ,DATABASEPROPERTYEX(DB_NAME(),'IsPublished') AS "IsPublished"
          ,DATABASEPROPERTYEX(DB_NAME(),'IsRecursiveTriggersEnabled') AS "IsRecursiveTriggersEnabled"
          ,DATABASEPROPERTYEX(DB_NAME(),'IsSubscribed') AS "IsSubscribed"
          ,DATABASEPROPERTYEX(DB_NAME(),'IsSyncWithBackup') AS "IsSyncWithBackup"
          ,DATABASEPROPERTYEX(DB_NAME(),'IsTornPageDetectionEnabled') AS "IsTornPageDetectionEnabled"
          ,DATABASEPROPERTYEX(DB_NAME(),'IsVerifiedClone') AS "IsVerifiedClone"
          ,DATABASEPROPERTYEX(DB_NAME(),'IsXTPSupported') AS "IsXTPSupported"
          ,DATABASEPROPERTYEX(DB_NAME(),'LastGoodCheckDbTime') AS "LastGoodCheckDbTime"
          ,DATABASEPROPERTYEX(DB_NAME(),'LCID') AS "LCID"
          ,DATABASEPROPERTYEX(DB_NAME(),'MaxSizeInBytes') AS "MaxSizeInBytes"
          ,DATABASEPROPERTYEX(DB_NAME(),'Recovery') AS "Recovery"
          ,DATABASEPROPERTYEX(DB_NAME(),'ServiceObjective') AS "ServiceObjective"
          ,DATABASEPROPERTYEX(DB_NAME(),'ServiceObjectiveId') AS "ServiceObjectiveId"
          ,DATABASEPROPERTYEX(DB_NAME(),'SQLSortOrder') AS "SQLSortOrder"
          ,DATABASEPROPERTYEX(DB_NAME(),'Status') AS "Status"
          ,DATABASEPROPERTYEX(DB_NAME(),'Updateability') AS "Updateability"
          ,DATABASEPROPERTYEX(DB_NAME(),'UserAccess') AS "UserAccess"
          ,DATABASEPROPERTYEX(DB_NAME(),'Version') AS "Version"
          FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
        ) "DATABASE_PROPERTIES"`;
       
const _SQL_CREATE_SAVE_POINT  = `SAVE TRANSACTION ${YadamuConstants.SAVE_POINT_NAME}`;

const _SQL_RESTORE_SAVE_POINT = `ROLLBACK TRANSACTION ${YadamuConstants.SAVE_POINT_NAME}`;
