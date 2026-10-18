
import YadamuConstants from '../../lib/yadamuConstants.js';

/*
**
** Markers on the DDL statements that must be deferred until the data has been loaded. They are SQL comments, so the DDL remains valid SQL.
**
*/

const POST_DATA_MARKER = '/* YADAMU:POST_DATA */ '
const DATA_MARKER      = '/* YADAMU:DATA */ '

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
/*
**
** Generate the DDL for every object in schema $1 that is not owned by an extension, in pg_dump order.
**
** The caller sets search_path to the schema being exported, so pg_get_*def(), format_type() and the reg* types leave the schema's own objects unqualified and qualify everything else.
** Objects named by the DDL itself are written as %%SCHEMA%%.name. Where a pg_get_*def() function always qualifies a name, the qualifier is rewritten.
**
** Statements that must run after the data has been loaded (indexes, foreign keys, triggers, rules, row level security, statistics and comments) are prefixed with the POST_DATA marker.
** Statements that copy state rather than structure (sequence values, materialized view contents) are prefixed with the DATA marker, and are not applied by DDL_ONLY operations.
**
*/
with recursive
target_schema as (
  select oid
       , pg_catalog.quote_ident($1) || '.' qualifier
    from pg_catalog.pg_namespace
   where nspname = $1
),
extension_members as (
  select classid, objid
    from pg_catalog.pg_depend
   where deptype = 'e'
),
tables as (
  select c.oid, c.relname, c.relkind, c.relispartition, c.relpersistence, c.relpartbound, c.reloftype, c.relrowsecurity, c.relforcerowsecurity
    from pg_catalog.pg_class c
   where c.relnamespace = (select oid from target_schema)
     and c.relkind in ('r','p')
     and (c.oid) not in (select objid from extension_members where classid = 'pg_catalog.pg_class'::regclass)
),
views as (
  select c.oid, c.relname, c.relkind, c.reloptions, c.relispopulated
    from pg_catalog.pg_class c
   where c.relnamespace = (select oid from target_schema)
     and c.relkind in ('v','m')
     and (c.oid) not in (select objid from extension_members where classid = 'pg_catalog.pg_class'::regclass)
),
view_dependencies as (
  select distinct rw.ev_class view_oid, d.refobjid referenced_oid
    from pg_catalog.pg_rewrite rw
    join pg_catalog.pg_depend d
      on d.classid = 'pg_catalog.pg_rewrite'::regclass and d.objid = rw.oid and d.refclassid = 'pg_catalog.pg_class'::regclass
   where rw.ev_class in (select oid from views)
     and d.refobjid in (select oid from views)
     and d.refobjid <> rw.ev_class
),
view_levels(oid, lvl) as (
  select oid, 0
    from views
  union all
  select vd.view_oid, vl.lvl + 1
    from view_dependencies vd
    join view_levels vl
      on vl.oid = vd.referenced_oid
   where vl.lvl < 100
),
view_order as (
  select oid, max(lvl) lvl
    from view_levels
   group by oid
),
sequences as (
  select s.oid, s.relname, q.seqtypid, q.seqincrement, q.seqmin, q.seqmax, q.seqstart, q.seqcache, q.seqcycle
    from pg_catalog.pg_class s
    join pg_catalog.pg_sequence q
      on q.seqrelid = s.oid
   where s.relnamespace = (select oid from target_schema)
     and s.relkind = 'S'
     and (s.oid) not in (select objid from extension_members where classid = 'pg_catalog.pg_class'::regclass)
     -- Identity columns create their own sequences
     and not exists (select 1 from pg_catalog.pg_depend d where d.classid = 'pg_catalog.pg_class'::regclass and d.objid = s.oid and d.deptype = 'i')
),
functions as (
  select p.oid
       , case when p.prokind = 'p' then 'PROCEDURE' else 'FUNCTION' end kind
       , pg_catalog.pg_get_functiondef(p.oid) definition
       , case
           when exists (select 1
                          from pg_catalog.pg_depend d
                          join pg_catalog.pg_type t on t.oid = d.refobjid
                          join pg_catalog.pg_class r on r.oid = t.typrelid
                         where d.classid = 'pg_catalog.pg_proc'::regclass and d.objid = p.oid and d.refclassid = 'pg_catalog.pg_type'::regclass and r.relkind in ('v','m'))
             then 55
           when exists (select 1
                          from pg_catalog.pg_depend d
                          join pg_catalog.pg_type t on t.oid = d.refobjid
                          join pg_catalog.pg_class r on r.oid = t.typrelid
                         where d.classid = 'pg_catalog.pg_proc'::regclass and d.objid = p.oid and d.refclassid = 'pg_catalog.pg_type'::regclass and r.relkind in ('r','p'))
             then 45
           else 30
         end phase
    from pg_catalog.pg_proc p
   where p.pronamespace = (select oid from target_schema)
     and p.prokind in ('f','p','w')
     and (p.oid) not in (select objid from extension_members where classid = 'pg_catalog.pg_proc'::regclass)
     -- Functions created implicitly, e.g. a range type's constructors
     and not exists (select 1 from pg_catalog.pg_depend d where d.classid = 'pg_catalog.pg_proc'::regclass and d.objid = p.oid and d.deptype = 'i')
),
user_types as (
  select t.oid, t.typname, t.typtype, t.typbasetype, t.typtypmod, t.typcollation, t.typnotnull, t.typdefaultbin, t.typrelid
    from pg_catalog.pg_type t
   where t.typnamespace = (select oid from target_schema)
     and t.typtype in ('e','d','c','r')
     and (t.oid) not in (select objid from extension_members where classid = 'pg_catalog.pg_type'::regclass)
     and (t.typtype <> 'c' or exists (select 1 from pg_catalog.pg_class c where c.oid = t.typrelid and c.relkind = 'c'))
),
table_columns as (
  select a.attrelid table_oid
       , pg_catalog.string_agg(
           pg_catalog.quote_ident(a.attname) || ' ' || pg_catalog.format_type(a.atttypid, a.atttypmod)
           || case when a.attcollation <> 0 and a.attcollation <> ty.typcollation then ' collate ' || a.attcollation::regcollation::text else '' end
           || case
                when a.attidentity in ('a','d')
                  then ' generated ' || case a.attidentity when 'a' then 'always' else 'by default' end || ' as identity'
                       || coalesce((select ' (start with ' || q.seqstart || ' increment by ' || q.seqincrement || ' minvalue ' || q.seqmin || ' maxvalue ' || q.seqmax || ' cache ' || q.seqcache || case when q.seqcycle then ' cycle' else ' no cycle' end || ')'
                                      from pg_catalog.pg_depend d
                                      join pg_catalog.pg_sequence q on q.seqrelid = d.objid
                                     where d.classid = 'pg_catalog.pg_class'::regclass and d.refobjid = a.attrelid and d.refobjsubid = a.attnum and d.deptype = 'i'), '')
                else ''
              end
           || case a.attgenerated
                when 's' then ' generated always as (' || pg_catalog.pg_get_expr(ad.adbin, ad.adrelid) || ') stored'
                when 'v' then ' generated always as (' || pg_catalog.pg_get_expr(ad.adbin, ad.adrelid) || ') virtual'
                else coalesce(' default ' || pg_catalog.pg_get_expr(ad.adbin, ad.adrelid), '')
              end
           || case when a.attnotnull then ' not null' else '' end
         , ', ' order by a.attnum) column_list
    from pg_catalog.pg_attribute a
    join pg_catalog.pg_type ty
      on ty.oid = a.atttypid
    left join pg_catalog.pg_attrdef ad
      on ad.adrelid = a.attrelid and ad.adnum = a.attnum
   where a.attrelid in (select oid from tables)
     and a.attnum > 0
     and not a.attisdropped
     -- Inherited columns are created by the parent table
     and a.attislocal
   group by a.attrelid
),
table_constraints as (
  -- Primary key, unique, check and exclusion constraints are created with the table. Foreign keys are added after the data has been loaded.
  select k.conrelid table_oid
       , pg_catalog.string_agg('constraint ' || pg_catalog.quote_ident(k.conname) || ' ' || pg_catalog.pg_get_constraintdef(k.oid)
                              , ', ' order by case k.contype when 'p' then 1 when 'u' then 2 when 'c' then 3 else 4 end, k.conname) constraint_list
    from pg_catalog.pg_constraint k
   where k.conrelid in (select oid from tables)
     and k.contype in ('p','u','c','x')
     and k.conislocal
     and k.conparentid = 0
     -- NOT VALID is ignored by create table, so check constraints that have not been validated are added after the data has been loaded.
     and k.convalidated
   group by k.conrelid
),
ddl_statements(phase, sub_order, object_oid, ddl) as (
  -- Collations
  select 5, 0, co.oid
       , 'create collation %%SCHEMA%%.' || pg_catalog.quote_ident(co.collname) || ' (provider = ' || case co.collprovider when 'i' then 'icu' when 'b' then 'builtin' else 'libc' end
         || case
              when co.collprovider in ('i','b') then ', locale = ' || pg_catalog.quote_literal(coalesce(x.collation ->> 'colllocale', x.collation ->> 'colliculocale'))
              else ', lc_collate = ' || pg_catalog.quote_literal(co.collcollate) || ', lc_ctype = ' || pg_catalog.quote_literal(co.collctype)
            end
         || case when not co.collisdeterministic then ', deterministic = false' else '' end
         || ')'
    from pg_catalog.pg_collation co
    -- The column holding the ICU locale depends on the server version
   cross join lateral (select to_jsonb(co) collation) x
   where co.collnamespace = (select oid from target_schema)
     and (co.oid) not in (select objid from extension_members where classid = 'pg_catalog.pg_collation'::regclass)
  union all
  -- Enumerated types
  select 10, 0, t.oid
       , 'create type %%SCHEMA%%.' || pg_catalog.quote_ident(t.typname) || ' as enum ('
         || coalesce((select pg_catalog.string_agg(pg_catalog.quote_literal(e.enumlabel), ', ' order by e.enumsortorder) from pg_catalog.pg_enum e where e.enumtypid = t.oid), '')
         || ')'
    from user_types t
   where t.typtype = 'e'
  union all
  -- Domains
  select 11, 0, t.oid
       , 'create domain %%SCHEMA%%.' || pg_catalog.quote_ident(t.typname) || ' as ' || pg_catalog.format_type(t.typbasetype, t.typtypmod)
         || case when t.typcollation <> 0 and t.typcollation <> bt.typcollation then ' collate ' || t.typcollation::regcollation::text else '' end
         || coalesce(' default ' || pg_catalog.pg_get_expr(t.typdefaultbin, 0), '')
         || case when t.typnotnull then ' not null' else '' end
         || coalesce((select pg_catalog.string_agg(' constraint ' || pg_catalog.quote_ident(k.conname) || ' ' || pg_catalog.pg_get_constraintdef(k.oid), '' order by k.conname)
                        from pg_catalog.pg_constraint k
                       where k.contypid = t.oid and k.contype = 'c'), '')
    from user_types t
    join pg_catalog.pg_type bt
      on bt.oid = t.typbasetype
   where t.typtype = 'd'
  union all
  -- Composite types
  select 12, 0, t.oid
       , 'create type %%SCHEMA%%.' || pg_catalog.quote_ident(t.typname) || ' as ('
         || coalesce((select pg_catalog.string_agg(pg_catalog.quote_ident(a.attname) || ' ' || pg_catalog.format_type(a.atttypid, a.atttypmod), ', ' order by a.attnum)
                        from pg_catalog.pg_attribute a
                       where a.attrelid = t.typrelid and a.attnum > 0 and not a.attisdropped), '')
         || ')'
    from user_types t
   where t.typtype = 'c'
  union all
  -- Range types
  select 13, 0, t.oid
       , 'create type %%SCHEMA%%.' || pg_catalog.quote_ident(t.typname) || ' as range (subtype = ' || pg_catalog.format_type(r.rngsubtype, null)
         || case when r.rngcollation <> 0 and r.rngcollation <> st.typcollation then ', collation = ' || r.rngcollation::regcollation::text else '' end
         || case when r.rngcanonical <> 0 then ', canonical = ' || r.rngcanonical::regproc::text else '' end
         || case when r.rngsubdiff <> 0 then ', subtype_diff = ' || r.rngsubdiff::regproc::text else '' end
         || ')'
    from user_types t
    join pg_catalog.pg_range r
      on r.rngtypid = t.oid
    join pg_catalog.pg_type st
      on st.oid = r.rngsubtype
   where t.typtype = 'r'
  union all
  -- Sequences, other than those that belong to identity columns
  select 20, 0, s.oid
       , 'create sequence %%SCHEMA%%.' || pg_catalog.quote_ident(s.relname) || ' as ' || pg_catalog.format_type(s.seqtypid, null)
         || ' increment by ' || s.seqincrement || ' minvalue ' || s.seqmin || ' maxvalue ' || s.seqmax || ' start with ' || s.seqstart || ' cache ' || s.seqcache
         || case when s.seqcycle then ' cycle' else ' no cycle' end
    from sequences s
  union all
  -- Functions and procedures. Those that use a table or view row type are created after the table or view.
  select f.phase, 0, f.oid
       , case
           when pg_catalog.starts_with(f.definition, 'CREATE OR REPLACE ' || f.kind || ' ' || (select qualifier from target_schema))
             then 'CREATE OR REPLACE ' || f.kind || ' %%SCHEMA%%.' || substr(f.definition, length('CREATE OR REPLACE ' || f.kind || ' ' || (select qualifier from target_schema)) + 1)
           else f.definition
         end
    from functions f
  union all
  -- Aggregates
  select 31, 0, p.oid
       , 'create aggregate %%SCHEMA%%.' || pg_catalog.quote_ident(p.proname) || '(' || pg_catalog.pg_get_function_identity_arguments(p.oid) || ') (sfunc = ' || a.aggtransfn::regproc::text
         || ', stype = ' || pg_catalog.format_type(a.aggtranstype, null)
         || case when a.aggfinalfn <> 0 then ', finalfunc = ' || a.aggfinalfn::regproc::text else '' end
         || case when a.aggcombinefn <> 0 then ', combinefunc = ' || a.aggcombinefn::regproc::text else '' end
         || coalesce(', initcond = ' || pg_catalog.quote_literal(a.agginitval), '')
         || ')'
    from pg_catalog.pg_proc p
    join pg_catalog.pg_aggregate a
      on a.aggfnoid = p.oid
   where p.pronamespace = (select oid from target_schema)
     and p.prokind = 'a'
     and a.aggkind = 'n'
     and (p.oid) not in (select objid from extension_members where classid = 'pg_catalog.pg_proc'::regclass)
  union all
  -- Tables. Partitions are created after the partitioned tables.
  select case when t.relispartition then 41 else 40 end, 0, t.oid
       , 'create ' || case when t.relpersistence = 'u' then 'unlogged ' else '' end || 'table if not exists %%SCHEMA%%.' || pg_catalog.quote_ident(t.relname)
         || case
              when t.relispartition
                then ' partition of ' || (select i.inhparent::regclass::text from pg_catalog.pg_inherits i where i.inhrelid = t.oid)
                     || coalesce(' (' || tk.constraint_list || ')', '')
                     || ' ' || pg_catalog.pg_get_expr(t.relpartbound, t.oid)
              when t.reloftype <> 0
                then ' of ' || pg_catalog.format_type(t.reloftype, null) || coalesce(' (' || tk.constraint_list || ')', '')
              else ' (' || concat_ws(', ', tc.column_list, tk.constraint_list) || ')'
                   || coalesce(' inherits (' || (select pg_catalog.string_agg(i.inhparent::regclass::text, ', ' order by i.inhseqno) from pg_catalog.pg_inherits i where i.inhrelid = t.oid) || ')', '')
            end
         || case when t.relkind = 'p' then ' partition by ' || pg_catalog.pg_get_partkeydef(t.oid) else '' end
    from tables t
    left join table_columns tc
      on tc.table_oid = t.oid
    left join table_constraints tk
      on tk.table_oid = t.oid
  union all
  -- Sequences owned by a column (serial columns)
  select 46, 0, s.oid
       , 'alter sequence %%SCHEMA%%.' || pg_catalog.quote_ident(s.relname) || ' owned by %%SCHEMA%%.' || pg_catalog.quote_ident(t.relname) || '.' || pg_catalog.quote_ident(a.attname)
    from sequences s
    join pg_catalog.pg_depend d
      on d.classid = 'pg_catalog.pg_class'::regclass and d.objid = s.oid and d.refclassid = 'pg_catalog.pg_class'::regclass and d.deptype = 'a'
    join tables t
      on t.oid = d.refobjid
    join pg_catalog.pg_attribute a
      on a.attrelid = t.oid and a.attnum = d.refobjsubid
  union all
  -- Views and materialized views, in dependency order. Materialized views are populated after the data has been loaded.
  select 50, vo.lvl, v.oid
       , case
           when v.relkind = 'v'
             then 'create or replace view %%SCHEMA%%.' || pg_catalog.quote_ident(v.relname) || coalesce(' with (' || pg_catalog.array_to_string(v.reloptions, ', ') || ')', '')
                  || ' as ' || pg_catalog.pg_get_viewdef(v.oid)
           else 'create materialized view if not exists %%SCHEMA%%.' || pg_catalog.quote_ident(v.relname)
                  || ' as ' || regexp_replace(pg_catalog.pg_get_viewdef(v.oid), ';\\s*$', '') || ' with no data'
         end
    from views v
    join view_order vo
      on vo.oid = v.oid
  union all
  -- Indexes that do not belong to a constraint. An index on a partitioned table is created on the partitions too, so the partitions' own copies are skipped.
  select 60, 0, i.indexrelid
       , case
           when strpos(x.ddl, ' ON ' || q.qualifier) > 0 then overlay(x.ddl placing ' ON %%SCHEMA%%.' from strpos(x.ddl, ' ON ' || q.qualifier) for length(' ON ' || q.qualifier))
           else x.ddl
         end
    from pg_catalog.pg_index i
   cross join target_schema q
   cross join lateral (select replace(pg_catalog.pg_get_indexdef(i.indexrelid), ' ON ONLY ', ' ON ') ddl) x
   where (i.indrelid in (select oid from tables) or i.indrelid in (select oid from views where relkind = 'm'))
     and not exists (select 1 from pg_catalog.pg_constraint k where k.conindid = i.indexrelid and k.conrelid = i.indrelid and k.contype in ('p','u','x'))
     and not exists (select 1 from pg_catalog.pg_inherits h where h.inhrelid = i.indexrelid)
  union all
  -- Extended statistics
  select 65, 0, sx.oid
       , case
           when strpos(y.ddl, ' FROM ' || q.qualifier) > 0 then overlay(y.ddl placing ' FROM %%SCHEMA%%.' from strpos(y.ddl, ' FROM ' || q.qualifier) for length(' FROM ' || q.qualifier))
           else y.ddl
         end
    from pg_catalog.pg_statistic_ext sx
   cross join target_schema q
   cross join lateral (select pg_catalog.pg_get_statisticsobjdef(sx.oid) ddl) x
   cross join lateral (select case
                                when pg_catalog.starts_with(x.ddl, 'CREATE STATISTICS ' || q.qualifier) then 'CREATE STATISTICS %%SCHEMA%%.' || substr(x.ddl, length('CREATE STATISTICS ' || q.qualifier) + 1)
                                else x.ddl
                              end ddl) y
   where sx.stxnamespace = q.oid
  union all
  -- Foreign keys
  select 70, 0, k.oid
       , 'alter table %%SCHEMA%%.' || pg_catalog.quote_ident(t.relname) || ' add constraint ' || pg_catalog.quote_ident(k.conname) || ' ' || pg_catalog.pg_get_constraintdef(k.oid)
    from pg_catalog.pg_constraint k
    join tables t
      on t.oid = k.conrelid
   where k.contype = 'f'
     and k.conparentid = 0
  union all
  -- Check constraints that have not been validated
  select 71, 0, k.oid
       , 'alter table %%SCHEMA%%.' || pg_catalog.quote_ident(t.relname) || ' add constraint ' || pg_catalog.quote_ident(k.conname) || ' ' || pg_catalog.pg_get_constraintdef(k.oid)
    from pg_catalog.pg_constraint k
    join tables t
      on t.oid = k.conrelid
   where k.contype = 'c'
     and k.conislocal
     and k.conparentid = 0
     and not k.convalidated
  union all
  -- Sequence values. Identity columns are reset from the data by resetIdentityColumns().
  select 76, 0, s.oid
       , 'select pg_catalog.setval(' || pg_catalog.quote_literal('%%SCHEMA%%.' || pg_catalog.quote_ident(s.relname)) || ', ' || ps.last_value || ', true)'
    from sequences s
    join pg_catalog.pg_sequences ps
      on ps.schemaname = $1 and ps.sequencename = s.relname
   where ps.last_value is not null
  union all
  -- Triggers, including triggers on views
  select 80, 0, tg.oid
       , case
           when strpos(x.ddl, ' ON ' || q.qualifier) > 0 then overlay(x.ddl placing ' ON %%SCHEMA%%.' from strpos(x.ddl, ' ON ' || q.qualifier) for length(' ON ' || q.qualifier))
           else x.ddl
         end
    from pg_catalog.pg_trigger tg
   cross join target_schema q
   cross join lateral (select pg_catalog.pg_get_triggerdef(tg.oid) ddl) x
   where (tg.tgrelid in (select oid from tables) or tg.tgrelid in (select oid from views))
     and not tg.tgisinternal
     and tg.tgparentid = 0
  union all
  select 81, 0, tg.oid
       , 'alter table %%SCHEMA%%.' || pg_catalog.quote_ident(t.relname) || ' disable trigger ' || pg_catalog.quote_ident(tg.tgname)
    from pg_catalog.pg_trigger tg
    join tables t
      on t.oid = tg.tgrelid
   where not tg.tgisinternal
     and tg.tgparentid = 0
     and tg.tgenabled = 'D'
  union all
  -- Rules, other than the rules that implement views
  select 82, 0, rw.oid
       , case
           when strpos(x.ddl, ' TO ' || q.qualifier) > 0 then overlay(x.ddl placing ' TO %%SCHEMA%%.' from strpos(x.ddl, ' TO ' || q.qualifier) for length(' TO ' || q.qualifier))
           else x.ddl
         end
    from pg_catalog.pg_rewrite rw
   cross join target_schema q
   cross join lateral (select regexp_replace(pg_catalog.pg_get_ruledef(rw.oid), ';\\s*$', '') ddl) x
   where (rw.ev_class in (select oid from tables) or rw.ev_class in (select oid from views))
     and rw.rulename <> '_RETURN'
  union all
  -- Row level security
  select 83, 0, t.oid
       , 'alter table %%SCHEMA%%.' || pg_catalog.quote_ident(t.relname) || ' enable row level security'
    from tables t
   where t.relrowsecurity
  union all
  select 83, 1, t.oid
       , 'alter table %%SCHEMA%%.' || pg_catalog.quote_ident(t.relname) || ' force row level security'
    from tables t
   where t.relforcerowsecurity
  union all
  select 84, 0, pol.oid
       , 'create policy ' || pg_catalog.quote_ident(pol.polname) || ' on %%SCHEMA%%.' || pg_catalog.quote_ident(t.relname)
         || case when not pol.polpermissive then ' as restrictive' else '' end
         || ' for ' || case pol.polcmd when 'r' then 'select' when 'a' then 'insert' when 'w' then 'update' when 'd' then 'delete' else 'all' end
         || ' to ' || case
                        when pol.polroles = '{0}' then 'public'
                        else (select pg_catalog.string_agg(pg_catalog.quote_ident(r.rolname), ', ' order by r.rolname) from pg_catalog.pg_roles r where r.oid = any(pol.polroles))
                      end
         || coalesce(' using (' || pg_catalog.pg_get_expr(pol.polqual, pol.polrelid) || ')', '')
         || coalesce(' with check (' || pg_catalog.pg_get_expr(pol.polwithcheck, pol.polrelid) || ')', '')
    from pg_catalog.pg_policy pol
    join tables t
      on t.oid = pol.polrelid
  union all
  -- Materialized view contents
  select 85, vo.lvl, v.oid
       , 'refresh materialized view %%SCHEMA%%.' || pg_catalog.quote_ident(v.relname)
    from views v
    join view_order vo
      on vo.oid = v.oid
   where v.relkind = 'm'
     and v.relispopulated
  union all
  -- Comments on relations and columns
  select 90, d.objsubid, d.objoid
       , 'comment on '
         || case
              when d.objsubid > 0 then 'column %%SCHEMA%%.' || pg_catalog.quote_ident(c.relname) || '.' || pg_catalog.quote_ident(a.attname)
              else case c.relkind when 'v' then 'view' when 'm' then 'materialized view' when 'S' then 'sequence' when 'i' then 'index' when 'I' then 'index' else 'table' end || ' %%SCHEMA%%.' || pg_catalog.quote_ident(c.relname)
            end
         || ' is ' || pg_catalog.quote_literal(d.description)
    from pg_catalog.pg_description d
    join pg_catalog.pg_class c
      on c.oid = d.objoid
    left join pg_catalog.pg_attribute a
      on a.attrelid = c.oid and a.attnum = d.objsubid
   where d.classoid = 'pg_catalog.pg_class'::regclass
     and (c.oid in (select oid from tables) or c.oid in (select oid from views) or c.oid in (select oid from sequences)
          or (c.relkind in ('i','I') and exists (select 1 from pg_catalog.pg_index i where i.indexrelid = c.oid and (i.indrelid in (select oid from tables) or i.indrelid in (select oid from views)))))
  union all
  -- Comments on types, functions, constraints and triggers
  select 91, 0, d.objoid
       , 'comment on ' || case t.typtype when 'd' then 'domain' else 'type' end || ' %%SCHEMA%%.' || pg_catalog.quote_ident(t.typname) || ' is ' || pg_catalog.quote_literal(d.description)
    from pg_catalog.pg_description d
    join user_types t
      on t.oid = d.objoid
   where d.classoid = 'pg_catalog.pg_type'::regclass
  union all
  select 92, 0, d.objoid
       , 'comment on ' || case p.prokind when 'p' then 'procedure' when 'a' then 'aggregate' else 'function' end || ' %%SCHEMA%%.' || pg_catalog.quote_ident(p.proname) || '(' || pg_catalog.pg_get_function_identity_arguments(p.oid) || ') is ' || pg_catalog.quote_literal(d.description)
    from pg_catalog.pg_description d
    join pg_catalog.pg_proc p
      on p.oid = d.objoid
   where d.classoid = 'pg_catalog.pg_proc'::regclass
     and p.pronamespace = (select oid from target_schema)
     and (p.oid) not in (select objid from extension_members where classid = 'pg_catalog.pg_proc'::regclass)
  union all
  select 93, 0, d.objoid
       , 'comment on constraint ' || pg_catalog.quote_ident(k.conname) || ' on %%SCHEMA%%.' || pg_catalog.quote_ident(t.relname) || ' is ' || pg_catalog.quote_literal(d.description)
    from pg_catalog.pg_description d
    join pg_catalog.pg_constraint k
      on k.oid = d.objoid
    join tables t
      on t.oid = k.conrelid
   where d.classoid = 'pg_catalog.pg_constraint'::regclass
  union all
  select 94, 0, d.objoid
       , 'comment on trigger ' || pg_catalog.quote_ident(tg.tgname) || ' on %%SCHEMA%%.' || pg_catalog.quote_ident(c.relname) || ' is ' || pg_catalog.quote_literal(d.description)
    from pg_catalog.pg_description d
    join pg_catalog.pg_trigger tg
      on tg.oid = d.objoid
    join pg_catalog.pg_class c
      on c.oid = tg.tgrelid
   where d.classoid = 'pg_catalog.pg_trigger'::regclass
     and (c.oid in (select oid from tables) or c.oid in (select oid from views))
)
select case
         when phase in (76,85) then '${DATA_MARKER}'
         when phase >= 60      then '${POST_DATA_MARKER}'
         else ''
       end || ddl
  from ddl_statements
 where ddl is not null
 order by phase, sub_order, object_oid`
 
  static #SQL_GENERATED_COLUMNS    = `select c.relname, a.attname from pg_catalog.pg_attribute a join pg_catalog.pg_class c on c.oid = a.attrelid join pg_catalog.pg_namespace n on n.oid = c.relnamespace where n.nspname = $1 and a.attgenerated <> '' and a.attnum > 0 and not a.attisdropped`

  // A consistent export reads every table from the snapshot exported by the manager, as pg_dump does.

  static #SQL_BEGIN_CONSISTENT_EXPORT    = `begin transaction isolation level repeatable read read only`

  static #SQL_EXPORT_SNAPSHOT            = `select pg_catalog.pg_export_snapshot()`

  static #SQL_END_CONSISTENT_EXPORT      = `commit transaction`

  static #SQL_INHERITANCE_PARENTS  =`select c.relname from pg_catalog.pg_class c join pg_catalog.pg_namespace n on n.oid = c.relnamespace where n.nspname = $1 and c.relkind = 'r' and exists (select 1 from pg_catalog.pg_inherits i where i.inhparent = c.oid)`

  static #SQL_SET_MAINTENANCE_WORK_MEM   =`select pg_catalog.set_config('maintenance_work_mem',$1,false)`

  static #SQL_RESET_MAINTENANCE_WORK_MEM = `reset maintenance_work_mem`

  static #SQL_GET_SEARCH_PATH      = `select pg_catalog.current_setting('search_path')`

  static #SQL_SET_SEARCH_PATH      = `select pg_catalog.set_config('search_path',$1,false)`

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
 
  static get SQL_GENERATED_COLUMNS()          { return PostgresStatementLibrary.#SQL_GENERATED_COLUMNS }
  static get SQL_BEGIN_CONSISTENT_EXPORT()    { return PostgresStatementLibrary.#SQL_BEGIN_CONSISTENT_EXPORT }
  static get SQL_EXPORT_SNAPSHOT()            { return PostgresStatementLibrary.#SQL_EXPORT_SNAPSHOT }
  static get SQL_END_CONSISTENT_EXPORT()      { return PostgresStatementLibrary.#SQL_END_CONSISTENT_EXPORT }
  // SET TRANSACTION SNAPSHOT does not accept a bind variable. The identifier comes from pg_export_snapshot() and is validated before use.
  static SQL_SET_TRANSACTION_SNAPSHOT(snapshotId) { return `set transaction snapshot '${snapshotId}'` }
  static get SQL_INHERITANCE_PARENTS()        { return PostgresStatementLibrary.#SQL_INHERITANCE_PARENTS }
  static get SQL_SET_MAINTENANCE_WORK_MEM()   { return PostgresStatementLibrary.#SQL_SET_MAINTENANCE_WORK_MEM }
  static get SQL_RESET_MAINTENANCE_WORK_MEM() { return PostgresStatementLibrary.#SQL_RESET_MAINTENANCE_WORK_MEM }
  static get SQL_GET_SEARCH_PATH()        { return PostgresStatementLibrary.#SQL_GET_SEARCH_PATH }
  static get SQL_SET_SEARCH_PATH()            { return PostgresStatementLibrary.#SQL_SET_SEARCH_PATH }

  static get POST_DATA_MARKER()               { return POST_DATA_MARKER }
  static get DATA_MARKER()                    { return DATA_MARKER }
 
  static get  DISABLE_FUNCTION_COMPILATION()   { return PostgresStatementLibrary.#DISABLE_FUNCTION_COMPILATION }
  static get  ENABLE_FUNCTION_COMPILATION()    { return PostgresStatementLibrary.#ENABLE_FUNCTION_COMPILATION }
}

export { PostgresStatementLibrary as default }

