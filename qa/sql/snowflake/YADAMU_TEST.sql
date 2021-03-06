create or replace procedure YADAMU_SYSTEM.PUBLIC.COMPARE_SCHEMAS(P_DATABASE STRING,P_SOURCE_SCHEMA STRING, P_TARGET_SCHEMA STRING, P_RULES STRING)
returns VARIANT
language javascript
as
$$

const rules = JSON.parse(P_RULES);
 
const timeStampLength = 20 + rules.timestampPrecision;
const SQL_LIST_COLUMNS = 
`with COLUMNS as (
 select distinct c.table_catalog, c.table_schema, c.table_name,column_name,ordinal_position,data_type,character_maximum_length,numeric_precision,numeric_scale,datetime_precision
       from "${P_DATABASE}".information_schema.columns c, "${P_DATABASE}".information_schema.tables t
       where t.table_name = c.table_name 
         and t.table_schema = c.table_schema
         and t.table_schema = '${P_SOURCE_SCHEMA}'
		 and t.table_type = 'BASE TABLE'
)
select table_name, listagg(
                     case 
                       when data_type = 'GEOGRAPHY' then
                         'ST_ASWKT("'  || column_name || '") "' || column_name || '"'
                       when data_type in ('TIMESTAMP_NTZ') then
                         'substr(to_char("' || column_name || '",''YYYY-MM-DD"T"HH24:MI:SS.FF9''),1,${timeStampLength}) "' || column_name || '"'
                       ${rules.emptyStringisNull ? `when data_type in ('TEXT','VARCHAR') then 'case when "' || column_name || '" = '''' then NULL else "' || column_name || '" end "' || column_name || '"'` : ''}
                       ${rules.infinityIsNull ? `when data_type in ('FLOAT') then 'case when "' || column_name || '" in (''INF'',''-INF'',''NAN'') then NULL else "' || column_name || '" end "' || column_name || '"'` : ''}
                       else 
                         '"' || column_name || '"'
                     end
					,','
				   ) within group (order by ordinal_position)
  from COLUMNS
  group by table_name;`
         
const results = []

  const tableList = snowflake.createStatement( { sqlText: SQL_LIST_COLUMNS}).execute()
  while (tableList.next()) {
    const tableName = tableList.getColumnValue(1); 
	const columns = tableList.getColumnValue(2);
    const compareStatement = `select (select count(*) from "${P_DATABASE}"."${P_SOURCE_SCHEMA}"."${tableName}")
                                    ,(select count(*) from "${P_DATABASE}"."${P_TARGET_SCHEMA}"."${tableName}")
                                    ,(select count(*) from (SELECT ${columns} from "${P_DATABASE}"."${P_SOURCE_SCHEMA}"."${tableName}" MINUS SELECT ${columns} from "${P_DATABASE}"."${P_TARGET_SCHEMA}"."${tableName}"))
                                    ,(select count(*) from (SELECT ${columns} from "${P_DATABASE}"."${P_TARGET_SCHEMA}"."${tableName}" MINUS SELECT ${columns} from "${P_DATABASE}"."${P_SOURCE_SCHEMA}"."${tableName}"))`
	
    try {	
	  const diffs = snowflake.createStatement({sqlText: compareStatement}).execute()
	  while (diffs.next()) {
	    const row = [P_SOURCE_SCHEMA,P_TARGET_SCHEMA,tableName,diffs.getColumnValue(1),diffs.getColumnValue(2),diffs.getColumnValue(3),diffs.getColumnValue(4),'',compareStatement]
	    results.push(row);
	  }									   
	} catch (e) {
	  const message = e.message.trim().replace(/\r/g,' ').replace(/\n/g,' ')
	  const row = [P_SOURCE_SCHEMA,P_TARGET_SCHEMA,tableName,'','','','',message]
	  results.push(row)
	}
  }			
  return results
  // return JSON.stringify(results);
$$
;
