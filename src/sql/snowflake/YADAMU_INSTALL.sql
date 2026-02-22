create or replace procedure YADAMU_SYSTEM.PUBLIC.INSTALL_HELPERS()
returns VARCHAR
language javascript
as $$
let uuid
try {
  const allocated = snowflake.createStatement( { sqlText: `call YADAMU_SYSTEM.PUBLIC.YADAMU_INSTANCE_ID()`}).execute()
  while (allocated.next()) {
     uuid = allocated.getColumnValue(1);
  }
} catch (e) {
  const generated = snowflake.createStatement( { sqlText: `select UUID_STRING()`}).execute()
  while (generated.next()) {
     uuid = generated.getColumnValue(1);
  }
  const statement = `create procedure YADAMU_SYSTEM.PUBLIC.YADAMU_INSTANCE_ID() returns TEXT language javascript as 'return "${uuid}";'`;
  snowflake.createStatement( { sqlText: statement }).execute()
}

const timestamp = new Date().toISOString()
const statement = `create or replace procedure YADAMU_SYSTEM.PUBLIC.YADAMU_INSTALLATION_TIMESTAMP() returns TEXT language javascript as 'return "${timestamp}";'`;
snowflake.createStatement( { sqlText: statement }).execute()
return 'Success'
$$
;
create or replace function YADAMU_SYSTEM.PUBLIC.RENDER_FLOAT(FLOAT_VALUE FLOAT)
returns VARCHAR
language javascript
as $$
  return FLOAT_VALUE ? FLOAT_VALUE.toExponential(20) : FLOAT_VALUE
$$;
--
CREATE OR REPLACE FUNCTION YADAMU_SYSTEM.IS_XML_COLUMN(
  p_schema VARCHAR,
  p_table VARCHAR, 
  p_column VARCHAR
)
RETURNS BOOLEAN
LANGUAGE JAVASCRIPT
AS
$$
  var full_table = '"' + P_SCHEMA + '"."' + P_TABLE + '"';
  
  var stmt = snowflake.createStatement({
    sqlText: `SELECT COUNT(*) as cnt
              FROM TABLE(INFORMATION_SCHEMA.DATA_METRIC_FUNCTION_EXPECTATIONS(
                REF_ENTITY_NAME => ?,
                REF_ENTITY_DOMAIN => 'table'
              ))
              WHERE expectation_expression ILIKE ?`,
    binds: [full_table, '%CHECK_XML("' + P_COLUMN + '")%']
  });
  
  try {
    var result = stmt.execute();
    result.next();
    return result.getColumnValue(1) > 0;
  } catch (e) {
    return false;
  }
$$;

