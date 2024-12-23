
import YadamuConstants from '../../lib/yadamuConstants.js';

class PostgresStatementLibrary {

  static #SQL_CONFIGURE_CONNECTION = `set timezone to 'UTC'; SET extra_float_digits to 3; SET Intervalstyle = 'iso_8601'`

  static #SQL_SCHEMA_INFORMATION   = `select * from YADAMU.YADAMU_EXPORT($1,$2,$3)`;
 
  static #SQL_SYSTEM_INFORMATION   = `select current_database() database_name,current_user, session_user, current_setting('server_version_num') database_version, right(to_char(current_timestamp,'YYYY-MM-DD"T"HH24:MI:SSTZH:TZM'),6) timezone, YADAMU.YADAMU_INSTANCE_ID() YADAMU_INSTANCE_ID, YADAMU.YADAMU_INSTALLATION_TIMESTAMP() YADAMU_INSTALLATION_TIMESTAMP, version() extended_version_information`;

  static #SQL_BEGIN_TRANSACTION    = `begin transaction`

  static #SQL_COMMIT_TRANSACTION   = `commit transaction`

  static #SQL_ROLLBACK_TRANSACTION = `rollback transaction`

  static #SQL_CREATE_SAVE_POINT    = `savepoint "${YadamuConstants.SAVE_POINT_NAME}"`

  static #SQL_RESTORE_SAVE_POINT   = `rollback to savepoint "${YadamuConstants.SAVE_POINT_NAME}"`

  static #SQL_RELEASE_SAVE_POINT   = `release savepoint "${YadamuConstants.SAVE_POINT_NAME}"`

  static #SQL_POSTGIS_INFO         = `select  PostGIS_version() "POSTGIS"`

  static #SQL_GET_IDENTITY_COLUMNS = `select concat('select setval(pg_get_serial_sequence(''', table_schema, '.', table_name,''',''', column_name,'''), coalesce(MAX(',column_name,'), 1)) from "', table_schema, '"."', table_name, '"') from information_schema.columns where table_schema = $1 and is_identity = 'YES'`
  
  static #SQL_GET_DDL_STATEMENTS   = `
with recursive dependencies as (
  select distinct o.oid                      object_id
       , o.relnamespace::regnamespace::text  object_schema_name
       , o.relname                           object_name
       , o.relkind                           object_type
  	   , ro.oid                              referenced_object_id
  	   , ro.relnamespace::regnamespace::text referenced_schema_name
  	   , ro.relname                          referenced_object_name
  	   , ro.relkind                          refereneced_object_type	 
    from pg_catalog.pg_rewrite rw
    join pg_catalog.pg_depend d 
      on rw.oid = d.objid
    join pg_catalog.pg_class o 
      on rw.ev_class = o.oid	
    join pg_catalog.pg_class ro
      on d.refobjid = ro.oid
   where o.oid != ro.oid
  union all
  select distinct o.oid                      object_id
       , o.relnamespace::regnamespace::text  object_schema_name4
       , o.relname                           object_name
  	 , o.relkind                           object_type
  	 , ro.oid                              referenced_object_id
  	 , ro.relnamespace::regnamespace::text referenced_schema_name
  	 , ro.relname                          referenced_object_name
  	 , ro.relkind                          refereneced_object_type	 
    from pg_catalog.pg_depend d 
    join pg_catalog.pg_class o 
      on d.objid = o.oid
    join pg_catalog.pg_class ro
      on d.refobjid = ro.oid
   where o.oid != ro.oid
  union all
      -- add constraints (primary keys, foreign keys, etc.)
  select co.oid as                           object_id
       , co.connamespace::regnamespace::text object_schema_name
  	 , co.conname                          object_name
       , 'c'                                 object_type
  	   , ro.oid                              referenced_object_id
  	   , ro.relnamespace::regnamespace::text referenced_schema_name
  	   , ro.relname                          referenced_object_name
       , ro.relkind                          refereneced_object_type	 
    from pg_catalog.pg_constraint co
    join pg_catalog.pg_class ro
  	  on co.conrelid = ro.oid
  union all
  select o.oid                               object_id
       , o.relnamespace::regnamespace::text  object_schema_name
       , o.relname                           object_name
       , o.relkind                           object_type
  	   , ro.oid                              referenced_object_id
  	   , ro.relnamespace::regnamespace::text referenced_schema_name
  	   , ro.relname                          referenced_object_name
  	   , ro.relkind                          refereneced_object_type	 
    from pg_catalog.pg_index ix
    join pg_catalog.pg_class o 
      on o.oid = ix.indexrelid
    join pg_catalog.pg_class ro
      on ro.oid = ix.indrelid
   where o.oid != ro.oid
),
dependency_tree as (
  select d.object_id
       , d.object_schema_name
       , d.object_name
       , d.object_type
       , d.referenced_object_id
       , d.referenced_schema_name
       , d.referenced_object_name
       , d.refereneced_object_type
       , array[d.object_id]::oid[] as dependency_path
    from dependencies d
  union all
    -- recursive step: add dependencies of the referenced objects
  select d.object_id
       , d.object_schema_name
       , d.object_name
       , d.object_type
       , d.referenced_object_id
       , d.referenced_schema_name
       , d.referenced_object_name
       , d.refereneced_object_type
       , dt.dependency_path || d.object_id as dependency_path
    from dependencies d
    join dependency_tree dt
      on d.object_id = dt.referenced_object_id
   where not d.object_id = any(dt.dependency_path) -- avoid cycles
),
roots as (
  select  o.oid as object_id
        , o.relnamespace::regnamespace::text as object_schema_name
        , o.relname as object_name
        , o.relkind as object_type
        , null::oid as referenced_object_id
        , null::text as referenced_schema_name
        , null::"name" as referenced_object_name
        , null::"char" as refereneced_object_type
        , null::oid[] as dependency_path
    from pg_catalog.pg_class o
    join pg_catalog.pg_namespace n on o.relnamespace = n.oid
    left join dependencies d on o.oid = d.object_id
    where d.object_id is null
),
all_objects as (
  -- combine roots and dependency tree
  select object_id
       , object_schema_name
       , object_name
       , object_type
       , referenced_object_id
       , referenced_schema_name
       , referenced_object_name
       , refereneced_object_type
       , dependency_path
    from roots
  union all
  select object_id
       , object_schema_name
       , object_name
       , object_type
       , referenced_object_id
       , referenced_schema_name
       , referenced_object_name
       , refereneced_object_type
	   , dependency_path
    from dependency_tree
  union all
  select p.oid                              as object_id
       , p.pronamespace::regnamespace::text as object_schema_name 
       , p.proname                          as object_name
       , p.prokind                          as object_type 
       , null::oid                          as referenced_object_id 
       , null::text                         as referenced_schema_name 
       , null::"name"                       as referenced_object_name
       , null::"char"                       as refereneced_object_type
       , array[4294967295]::oid[]               as dependency_path 
    from pg_catalog.pg_proc p

), 
tables as (
  select oid as "table_oid"
       , relname as "table_name"
	   , relnamespace as "namespace_oid"
	   , relnamespace::regnamespace::text as "schema_name"
	   , relkind as "object_type"
   from  pg_catalog.pg_class r
   where relkind = 'r'
),
table_columns as (
  select t.table_oid
       , a.attname as column_name
	   , a.attnum as "column_number"
	   , pg_catalog.format_type(a.atttypid, a.atttypmod) as data_type
	   , not a.attnotnull as is_nullable
	   , pg_get_expr(ad.adbin, ad.adrelid) as column_default
	   , case
           when a.attidentity = 'a' 
		     then 'generated always as identity'
           when a.attidentity = 'd' 
		     then 'generated by default as identity'
           else 
		     null
         end as identity_type
    from pg_catalog.pg_attribute a
    left join pg_attrdef ad 
      on a.attnum = ad.adnum and a.attrelid = ad.adrelid
    join tables t 
      on t.table_oid = a.attrelid
    where a.attnum > 0
      and not a.attisdropped
),
table_constraints as (
  select t.table_oid
       , oid                              as constraint_id
       , conname                          as constraint_name
	   , connamespace::regnamespace::text as "schema_name"
	   , conindid                         as constraint_index_id
	   , pg_get_constraintdef(c.oid, true) as constraint_definition
  from pg_catalog.pg_constraint c
  join tables t 
    on t.table_oid = c.conrelid
),
table_indexes as (
  select t.table_oid
       , indexrelid                       as index_id
       , relname                          as index_name
	   , relnamespace::regnamespace::text as "schema_name"
   from pg_catalog.pg_index i
   join tables t 
     on t.table_oid = i.indrelid
   join pg_class c
     on c.oid = i.indexrelid 
  where indexrelid not in (select constraint_index_id from table_constraints)
),
table_sequences as (
   select s.oid                           as sequence_id
     from pg_class s
     join pg_sequence seq 
	   on s.oid = seq.seqrelid
     join pg_depend d 
	   on s.oid = d.objid
     join pg_class t 
	   on d.refobjid = t.oid
     join pg_attribute a 
	   on a.attrelid = t.oid and a.attnum = d.refobjsubid
    where d.deptype = 'i'  -- dependency type 'i' indicates an identity column
     and s.relkind = 'S' -- only sequences
),
table_ddl as (
  select t.table_oid
       ,  ' (' || string_agg(column_def, ', ' order by column_number) || coalesce(', ' || string_agg(constraint_def, ', '), '') || ');' as ddl_statement
    from (
      select table_oid
	       , column_number
		   , quote_ident(column_name) || ' ' || data_type || coalesce(' ' || identity_type, '') || coalesce(' default ' || column_default, '') ||  case when is_nullable then '' else ' not null' end as column_def
		   , null as constraint_def
        from table_columns
      union all
        select table_oid
		     , null as column_number
			 , null as column_def
			 , 'constraint ' || quote_ident(constraint_name) || ' ' || constraint_definition as constraint_def
        from table_constraints
    ) p
    join tables t 
      on t.table_oid = p.table_oid
   group by t.table_oid, schema_name, table_name
)
select case 
         when object_type = 'r' 
	  	   then 'create table if not exists %%SCHEMA%%.' || quote_ident(object_name) || (select ddl_statement from table_ddl d where d.table_oid = object_id)
         when object_type = 'v' 
		   then 'create or replace view %%SCHEMA%%.' || quote_ident(object_name) || ' as ' || pg_get_viewdef(object_id)
         when object_type = 'f' 
		   then pg_get_functiondef(object_id)
         when object_type = 'p' 
		   then pg_get_functiondef(object_id)
         else 
		   null
       end as ddl 
  from (
    select o.object_id
	     , o.object_name
		 , o.object_type
		 , coalesce((select max(oid) FROM unnest(dependency_path) as oid), 0) as max_dependency_path
	  from all_objects o
     where object_schema_name = $1
	   and object_id not in (select constraint_id from table_constraints)
	   and object_id not in (select constraint_index_id from table_constraints)
	   and object_id not in (select sequence_id from table_sequences)
	 group by o.object_id, o.object_name, o.object_type, dependency_path
   )
 order by max_dependency_path`
 
  static #DISABLE_FUNCTION_COMPILATION = `set check_function_bodies = false`
  static #ENABLE_FUNCTION_COMPILATION  = `set check_function_bodies = true`
  
  static get SQL_CONFIGURE_CONNECTION()       { return PostgresStatementLibrary.#SQL_CONFIGURE_CONNECTION }
  static get SQL_SYSTEM_INFORMATION()         { return PostgresStatementLibrary.#SQL_SYSTEM_INFORMATION }
  static get SQL_SCHEMA_INFORMATION()         { return PostgresStatementLibrary.#SQL_SCHEMA_INFORMATION } 
  static get SQL_BEGIN_TRANSACTION()          { return PostgresStatementLibrary.#SQL_BEGIN_TRANSACTION }
  static get SQL_COMMIT_TRANSACTION()         { return PostgresStatementLibrary.#SQL_COMMIT_TRANSACTION }
  static get SQL_ROLLBACK_TRANSACTION()       { return PostgresStatementLibrary.#SQL_ROLLBACK_TRANSACTION }
  static get SQL_CREATE_SAVE_POINT()          { return PostgresStatementLibrary.#SQL_CREATE_SAVE_POINT }  
  static get SQL_RESTORE_SAVE_POINT()         { return PostgresStatementLibrary.#SQL_RESTORE_SAVE_POINT }
  static get SQL_RELEASE_SAVE_POINT()         { return PostgresStatementLibrary.#SQL_RELEASE_SAVE_POINT }
  static get SQL_POSTGIS_INFO()               { return PostgresStatementLibrary.#SQL_POSTGIS_INFO }
  static get SQL_GET_IDENTITY_COLUMNS()       { return PostgresStatementLibrary.#SQL_GET_IDENTITY_COLUMNS }
  static get SQL_GET_DDL_STATEMENTS()         { return PostgresStatementLibrary.#SQL_GET_DDL_STATEMENTS }
 
  static get  DISABLE_FUNCTION_COMPILATION()   { return PostgresStatementLibrary.#DISABLE_FUNCTION_COMPILATION }
  static get  ENABLE_FUNCTION_COMPILATION()    { return PostgresStatementLibrary.#ENABLE_FUNCTION_COMPILATION }
}

export { PostgresStatementLibrary as default }

