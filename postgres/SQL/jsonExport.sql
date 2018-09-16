create or replace function jsonExport(P_SCHEMA VARCHAR)
returns setof jsonb
as $$
declare
  V_STATEMENT    TEXT;
  V_FIRST_TABLE  BOOLEAN := TRUE;
  T              RECORD;
begin
  V_STATEMENT := 'select jsonb_build_object(''data'',
                           jsonb_build_object(';
  for t in select table_schema, table_name, string_agg('"' || column_name || '"',',') SELECT_LIST
             from information_schema.columns 
            where table_schema = P_SCHEMA
         group by table_schema, table_name loop
    if (NOT V_FIRST_TABLE) then
	  V_STATEMENT := V_STATEMENT || ',';
	end if;
	V_FIRST_TABLE := FALSE;
    V_STATEMENT := V_STATEMENT || '''' || t.table_name || ''',(select jsonb_agg(jsonb_build_array(' || t.SELECT_LIST ||')) from "' || t.table_schema || '"."' || t.table_name || '")';
  end loop; 
  V_STATEMENT := V_STATEMENT || '))';
  return query execute V_STATEMENT;
end;
$$ LANGUAGE plpgsql;