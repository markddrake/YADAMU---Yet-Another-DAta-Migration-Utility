/*
**
** Drop Yadmau Functions created in system schema
**
*/
do 
$$
declare
   _count numeric;
   _sql text;
begin
   SELECT count(*)::int
        , 'DROP FUNCTION ' || string_agg(oid::regprocedure::text, '; DROP FUNCTION ')
   FROM   pg_proc
   WHERE  UPPER(PRONAME) IN ('YADAMU_INSTANCE_ID', 'YADAMU_INSTALLATION_TIMESTAMP' ,'YADAMU_ASPOINTARRAY', 'YADAMU_ASPOINT', 'YADAMU_ASBOX', 'YADAMU_ASPATH', 'YADAMU_ASPOLOYGON', 'AS_JSON', 'AS_TS_VECTOR', 'AS_JSON', 'AS_RANGE', 'YADAMU_ASGEOJSON', 'YADAMU_ASLINE', 'YADAMU_ASGEOJSON', 'YADAMU_ASCIRCLE', 'YADAMU_ASLSEG', 'YADAMU_EXPORT', 'MAP_PGSQL_DATA_TYPE', 'GENERATE_SQL', 'YADAMU_IMPORT_JSONB', 'YADAMU_IMPORT_JSON', 'GENERATE_STATEMENTS','MAP_FOREIGN_DATA_TYPE','EXPORT_JSON','IMPORT_JSON','IMPORT_JSONB')
     and  UPPER(pronamespace::regnamespace::text) <> 'YADAMU'
	 and  prokind = 'f'
   INTO   _count, _sql;  -- only returned if trailing DROPs succeed
   if _count > 0 then
     execute _sql;
   end if;
   SELECT count(*)::int
        , 'DROP PROCEDURE ' || string_agg(oid::regprocedure::text, '; DROP PROCEDURE ')
   FROM   pg_proc
   WHERE  UPPER(proname) in ('SET_VENDOR_TYPE_MAPPINGS')
     and  UPPER(pronamespace::regnamespace::text) <> 'YADAMU'
	 and  prokind = 'p'
   INTO   _count, _sql;  -- only returned if trailing DROPs succeed

   if _count > 0 then
     execute _sql;
   end if;
end
$$ 
language plpgsql;
--
/*
**
** Drop Yadmau QA Functions and procedures
**
*/
do 
$$
declare
   _count numeric;
   _sql text;
begin
   SELECT count(*)::int
        , 'DROP FUNCTION ' || string_agg(oid::regprocedure::text, '; DROP FUNCTION ')
   FROM   pg_proc
   WHERE   UPPER(proname) in ('APPLY_XML_RULE','XML_NORMALIZE','TRUNCATE_GEOMETRY_WKT','GENERATE_COMPARE_COLUMNS')
     and  UPPER(pronamespace::regnamespace::text) <> 'YADAMU'
	 and  prokind = 'f'
   INTO   _count, _sql;  -- only returned if trailing DROPs succeed
   if _count > 0 then
     execute _sql;
   end if;
   SELECT count(*)::int
        , 'DROP PROCEDURE ' || string_agg(oid::regprocedure::text, '; DROP PROCEDURE ')
   FROM   pg_proc
   WHERE  UPPER(proname) in ('COMPARE_SCHEMA')
     and  UPPER(pronamespace::regnamespace::text) <> 'YADAMU'
	 and  prokind = 'p'
   INTO   _count, _sql;  -- only returned if trailing DROPs succeed

   if _count > 0 then
     execute _sql;
   end if;
end
$$ language plpgsql;
