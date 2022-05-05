"use strict" 

class TeradataStatementLibrary {

  static get DATABASE_VERSION()                               { return 19 }

  static get SQL_SYSTEM_INFORMATION()                         { return _SQL_SYSTEM_INFORMATION }
  static get SQL_SCHEMA_INFORMATION()                         { return _SQL_SCHEMA_INFORMATION }
  static get SQL_BEGIN_TRANSACTION()                          { return _SQL_BEGIN_TRANSACTION }  
  static get SQL_COMMIT_TRANSACTION()                         { return _SQL_COMMIT_TRANSACTION }
  static get SQL_ROLLBACK_TRANSACTION()                       { return _SQL_ROLLBACK_TRANSACTION }

}
 
export {TeradataStatementLibrary  as default }

// SQL Statements

const _SQL_SYSTEM_INFORMATION = `{fn teradata_nativesql}Database version {fn teradata_database_version}`;

// const SQL_SYSTEM_INFORMATION = `help session`;

const _SQL_SCHEMA_INFORMATION = 
`select TRIM(c.DatabaseName) "TABLE_SCHEMA"
       ,TRIM(c.TableName)    "TABLE_NAME"
       ,'[' || TRIM(TRAILING ',' FROM (XMLAGG('"' || TRIM(ColumnName) || '",' ORDER BY ColumnId) (VARCHAR(32000)))) || ']' "COLUMN_NAME_ARRAY"
       ,'[' || TRIM(TRAILING ',' FROM (XMLAGG('"' || case when ColumnType = 'UD' then TRIM(ColumnUDTName) else  TRIM(ColumnType) end || '",' ORDER BY ColumnId) (VARCHAR(32000))))  || ']' "DATA_TYPE_ARRAY"
       ,'[' || TRIM(TRAILING ',' FROM (XMLAGG(case
	                                     when (ColumnType in ('AT','SZ','TS','TZ')) then
										    '[' || TRIM(DecimalFractionalDigits) || '],'
										 when (ColumnType = 'DA') then
										    '[],'
                                         when DecimalTotalDigits is not NULL then 
                                           case 
                                             when DecimalTotalDigits = -128 and DecimalFractionalDigits = -128 then
                                               '[],'
                                             when DecimalFractionalDigits is not null then
                                               '[' || TRIM(DecimalTotalDigits) || ',' || TRIM(DecimalFractionalDigits) || '],'
                                             else 
                                               '[' || TRIM(DecimalTotalDigits) || '],'
                                           end
                                         when ColumnLength is not NULL then 
                                           '[' || cast(cast(ColumnLength as INT) as VARCHAR(256)) || '],'
                                         else 
                                          '[],'
                                       end 
                                     ORDER BY ColumnId) (VARCHAR(32000)))) || ']' "SIZE_CONSTRAINT_ARRARY"
       ,TRIM(TRAILING ',' FROM (XMLAGG('"' || TRIM(ColumnName) || '",' ORDER BY ColumnId) (VARCHAR(32000)))) "CLIENT_SELECT_LIST"
  from DBC.ColumnsVX c
  JOIN DBC.TablesVX t 
    on c.DatabaseName = t.DatabaseName
   and c.TableName = t.TableName
 where t.DatabaseName = ?
  group by c.DatabaseName, c.TableName`;
  
const _SQL_BEGIN_TRANSACTION    = `{fn teradata_nativesql}{fn teradata_autocommit_off}`;

const _SQL_COMMIT_TRANSACTION   = `{fn teradata_commit}`;

const _SQL_ROLLBACK_TRANSACTION = `{fn teradata_rollback}`;




