create or replace procedure YADAMU_SYSTEM.PUBLIC.INSTALL_HELPERS()
returns VARCHAR
language javascript
as
$$
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

