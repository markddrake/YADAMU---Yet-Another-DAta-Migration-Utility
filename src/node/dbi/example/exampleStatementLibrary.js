class ExampleStatementLibrary {

  static get SQL_CONFIGURE_CONNECTION()                       { return _SQL_CONFIGURE_CONNECTION }
  static get SQL_SYSTEM_INFORMATION()                         { return _SQL_SYSTEM_INFORMATION }
  static get SQL_SCHEMA_METADATA()                            { return _SQL_SCHEMA_METADATA }
  static get SQL_GENERATE_STATEMENTS()                        { return _SQL_GENERATE_STATEMENTS }

  static get SQL_BEGIN_TRANSACTION()                          { return _SQL_BEGIN_TRANSACTION }  
  static get SQL_COMMIT_TRANSACTION()                         { return _SQL_COMMIT_TRANSACTION }
  static get SQL_ROLLBACK_TRANSACTION()                       { return _SQL_ROLLBACK_TRANSACTION }

}


const _SQL_CONFIGURE_CONNECTION = `-- SQL TO CONFIGURE THE CONNECTION`

const _SQL_SYSTEM_INFORMATION   = `-- SQL TO FETCH SYSTEM INFORMATION`

/*
**
** Using INFORMATION_SCHEMA 
**

`select t.table_schema   "TABLE_SCHEMA"
                     ,t.table_name   "TABLE_NAME"
                     ,concat('[',listagg(concat('"',c.column_name,'"'),',') within group (order by ordinal_position),']') "COLUMN_NAME_ARRAY"
                     ,concat('[',listagg(concat('"',case when c.comment = concat('CHECK(CHECK_XML("',c.column_name,'") IS NULL)') then 'XML' else data_type end,'"'),',') within group (order by ordinal_position),']') "DATA_TYPE_ARRAY"
                     ,concat('[',listagg(case
                                   when (numeric_precision is not null) and (numeric_scale is not null) 
                                     then concat('[',numeric_precision,',',numeric_scale,']')
                                   when (numeric_precision is not null) 
                                     then concat('[',numeric_precision,']')
                                   when (datetime_precision is not null)
                                     then concat('[',datetime_precision,']')
                                   when (character_maximum_length is not null)
                                     then concat('[',character_maximum_length,']')
                                   else
                                     '[]'
                                 end
                                ,','                   
                               ) within group (order by ordinal_position)
                             ,']') "SIZE_CONSTRAINT_ARRAY"
                     ,listagg(case
                               when c.data_type = 'VARIANT' then
                                 concat('TO_VARCHAR("',column_name,'") "',column_name,'"')
                               when c.data_type = 'TIME' then
                                 concat('cast(concat(''1971-01-01T'',to_varchar("',column_name,'",''HH24:MI:SS.FF9'')) as Timestamp)')
							   when c.data_type in ('FLOAT','FLOAT4','FLOAT8','DOUBLE','DOUBLE PRECISION','REAL') then
							     concat('TO_VARCHAR("',column_name,'",''TME'') "',column_name,'"')
                               else
                                 concat('"',column_name,'"')
                               end
                              ,','
                             ) within group (order by ordinal_position) "CLIENT_SELECT_LIST"
                 from "INFORMATION_SCHEMA"."COLUMNS" c, "INFORMATION_SCHEMA"."TABLES" t
                where t.table_name = c.table_name
                  and t.table_schema = c.table_schema
                  and t.table_type = 'BASE TABLE'
                  and t.table_schema = ?
                group by t.table_schema, t.table_name`;
				
**
*/

const _SQL_SCHEMA_METADATA      = `-- SQL TO FETCH METADATA FOR THE TARGET SCHEMA`

const _SQL_GENERATE_STATEMENTS  = `-- SQL TO GENERATE DDL AND DML STATEMENTS`

const _SQL_BEGIN_TRANSACTION    = `BEGIN TRANSACTION`;

const _SQL_COMMIT_TRANSACTION   = `COMMIT TRANSACTION`;

const _SQL_ROLLBACK_TRANSACTION = `ROLLBACK TRANSACTION`;

export { ExampleStatementLibrary as default }
