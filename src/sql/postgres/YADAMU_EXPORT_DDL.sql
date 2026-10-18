/*
**
** YADAMU_EXPORT_DDL - PostgreSQL DDL Export
**
** Generates DDL statements for all objects in a given schema.
** Equivalent to the Oracle YADAMU_EXPORT_DDL package.
**
** Usage:
**   SELECT * FROM YADAMU_EXPORT_DDL.FETCH_DDL_STATEMENTS('my_schema');
**
*/
--
create schema if not exists YADAMU_EXPORT_DDL;
--
CREATE OR REPLACE FUNCTION YADAMU_EXPORT_DDL.FETCH_DDL_STATEMENTS(P_SCHEMA text)
returns SETOF text
as
$$
declare
  V_DDL_STATEMENT     text;
  V_TABLE_NAME        text;
  V_COLUMN_LIST       text;
  V_PK_COLUMNS        text;
  V_UQ_COLUMNS        text;
  V_CONSTRAINT_NAME   text;
  V_SCHEMA_OID        oid;
  V_NAMESPACE         text;
  r                   record;
begin
  --
  -- Resolve the schema OID
  --
  SELECT n.oid
    INTO V_SCHEMA_OID
    FROM pg_namespace n
   WHERE n.nspname = P_SCHEMA;
  --
  if V_SCHEMA_OID is null then
    RAISE EXCEPTION 'Schema "%" does not exist', P_SCHEMA;
  end if;
  --
  -- =============================================
  -- 1. TYPES (enums, composites, domains)
  -- =============================================
  --
  -- Enum types
  --
  for r in
    select t.typname as type_name,
           string_agg(quote_literal(e.enumlabel) order by e.enumsortorder, ', ') as enum_labels
      from pg_type t
      join pg_enum e on e.enumtypid = t.oid
     where t.typnamespace = V_SCHEMA_OID
     group by t.typname
     order by t.typname
  loop
    return next 'CREATE TYPE ' || quote_ident(P_SCHEMA) || '.' || quote_ident(r.type_name) || ' AS ENUM (' || r.enum_labels || ')';
  end loop;
  --
  -- Composite types
  --
  for r in
    select t.typname as type_name,
           string_agg(
             quote_ident(a.attname) || ' ' || pg_catalog.format_type(a.atttypid, a.atttypmod)
             order by a.attnum, ', '
           ) as columns
      from pg_type t
      join pg_class c on c.oid = t.typrelid
      join pg_attribute a on a.attrelid = c.oid and a.attnum > 0 and not a.attisdropped
     where t.typnamespace = V_SCHEMA_OID
       and t.typtype = 'c'
       and c.relkind = 'c'
     group by t.typname
     order by t.typname
  loop
    return next 'CREATE TYPE ' || quote_ident(P_SCHEMA) || '.' || quote_ident(r.type_name) || ' AS (' || r.columns || ')';
  end loop;
  --
  -- Domains
  --
  for r in
    select t.typname as domain_name,
           pg_catalog.format_type(t.typbasetype, t.typtypmod) as base_type,
           t.typnotnull as is_not_null,
           t.typdefault as default_value,
           (select string_agg(pg_get_constraintdef(con.oid), ' ')
              from pg_constraint con
             where con.contypid = t.oid) as check_constraints
      from pg_type t
     where t.typnamespace = V_SCHEMA_OID
       and t.typtype = 'd'
     order by t.typname
  loop
    V_DDL_STATEMENT := 'CREATE DOMAIN ' || quote_ident(P_SCHEMA) || '.' || quote_ident(r.domain_name) || ' AS ' || r.base_type;
    if r.default_value is not null then
      V_DDL_STATEMENT := V_DDL_STATEMENT || ' DEFAULT ' || r.default_value;
    end if;
    if r.is_not_null then
      V_DDL_STATEMENT := V_DDL_STATEMENT || ' NOT NULL';
    end if;
    if r.check_constraints is not null then
      V_DDL_STATEMENT := V_DDL_STATEMENT || ' ' || r.check_constraints;
    end if;
    return next V_DDL_STATEMENT;
  end loop;
  --
  -- =============================================
  -- 2. SEQUENCES
  -- =============================================
  --
  for r in
    select s.sequencename,
           s.data_type,
           s.start_value,
           s.min_value,
           s.max_value,
           s.increment_by,
           s.cycle as is_cycle,
           s.cache_size
      from pg_sequences s
     where s.schemaname = P_SCHEMA
     order by s.sequencename
  loop
    V_DDL_STATEMENT := 'CREATE SEQUENCE ' || quote_ident(P_SCHEMA) || '.' || quote_ident(r.sequencename);
    if r.data_type is not null and r.data_type <> 'bigint' then
      V_DDL_STATEMENT := V_DDL_STATEMENT || ' AS ' || r.data_type;
    end if;
    V_DDL_STATEMENT := V_DDL_STATEMENT || ' INCREMENT BY ' || r.increment_by;
    V_DDL_STATEMENT := V_DDL_STATEMENT || ' MINVALUE ' || r.min_value;
    V_DDL_STATEMENT := V_DDL_STATEMENT || ' MAXVALUE ' || r.max_value;
    V_DDL_STATEMENT := V_DDL_STATEMENT || ' START WITH ' || r.start_value;
    if r.cache_size > 1 then
      V_DDL_STATEMENT := V_DDL_STATEMENT || ' CACHE ' || r.cache_size;
    else
      V_DDL_STATEMENT := V_DDL_STATEMENT || ' NO CACHE';
    end if;
    if r.is_cycle then
      V_DDL_STATEMENT := V_DDL_STATEMENT || ' CYCLE';
    else
      V_DDL_STATEMENT := V_DDL_STATEMENT || ' NO CYCLE';
    end if;
    return next V_DDL_STATEMENT;
  end loop;
  --
  -- =============================================
  -- 3. TABLES (with inline PRIMARY KEY and UNIQUE)
  -- =============================================
  --
  for V_TABLE_NAME in
    select c.relname
      from pg_class c
     where c.relnamespace = V_SCHEMA_OID
       and c.relkind = 'r'
       and c.relname not like 'pg_%'
     order by c.relname
  loop
    --
    -- Build column definitions
    --
    select string_agg(
             quote_ident(a.attname) || ' ' || pg_catalog.format_type(a.atttypid, a.atttypmod)
             || case when a.attidentity = 'a' then ' GENERATED ALWAYS AS IDENTITY'
                     when a.attidentity = 'd' then ' GENERATED BY DEFAULT AS IDENTITY'
                     else ''
                end
             || case when a.atthasdef and a.attidentity = '' then ' DEFAULT ' || pg_get_expr(d.adbin, d.adrelid)
                     else ''
                end
             || case when a.attnotnull then ' NOT NULL' else '' end
             || case when a.attgenerated = 's' then ' GENERATED ALWAYS AS (' || pg_get_expr(d.adbin, d.adrelid) || ') STORED'
                     else ''
                end,
             E',\n  '
             order by a.attnum
           )
      into V_COLUMN_LIST
      from pg_attribute a
      left join pg_attrdef d on d.adrelid = a.attrelid and d.adnum = a.attnum
     where a.attrelid = (quote_ident(P_SCHEMA) || '.' || quote_ident(V_TABLE_NAME))::regclass
       and a.attnum > 0
       and not a.attisdropped
       and a.attgenerated <> 's';
    --
    -- Also include generated columns
    --
    select string_agg(
             quote_ident(a.attname) || ' ' || pg_catalog.format_type(a.atttypid, a.atttypmod)
             || ' GENERATED ALWAYS AS (' || pg_get_expr(d.adbin, d.adrelid) || ') STORED',
             E',\n  '
             order by a.attnum
           )
      into V_DDL_STATEMENT
      from pg_attribute a
      left join pg_attrdef d on d.adrelid = a.attrelid and d.adnum = a.attnum
     where a.attrelid = (quote_ident(P_SCHEMA) || '.' || quote_ident(V_TABLE_NAME))::regclass
       and a.attnum > 0
       and not a.attisdropped
       and a.attgenerated = 's';
    --
    if V_DDL_STATEMENT is not null then
      V_COLUMN_LIST := V_COLUMN_LIST || E',\n  ' || V_DDL_STATEMENT;
    end if;
    --
    -- Inline PRIMARY KEY constraint
    --
    select con.conname,
           string_agg(quote_ident(a.attname), ', ' order by array_position(con.conkey, a.attnum))
      into V_CONSTRAINT_NAME, V_PK_COLUMNS
      from pg_constraint con
      join pg_attribute a on a.attrelid = con.conrelid and a.attnum = any(con.conkey)
     where con.conrelid = (quote_ident(P_SCHEMA) || '.' || quote_ident(V_TABLE_NAME))::regclass
       and con.contype = 'p'
     group by con.conname;
    --
    if V_PK_COLUMNS is not null then
      V_COLUMN_LIST := V_COLUMN_LIST || E',\n  CONSTRAINT ' || quote_ident(V_CONSTRAINT_NAME) || ' PRIMARY KEY (' || V_PK_COLUMNS || ')';
    end if;
    --
    -- Inline UNIQUE constraints
    --
    for r in
      select con.conname,
             string_agg(quote_ident(a.attname), ', ' order by array_position(con.conkey, a.attnum)) as uq_columns
        from pg_constraint con
        join pg_attribute a on a.attrelid = con.conrelid and a.attnum = any(con.conkey)
       where con.conrelid = (quote_ident(P_SCHEMA) || '.' || quote_ident(V_TABLE_NAME))::regclass
         and con.contype = 'u'
       group by con.conname
       order by con.conname
    loop
      V_COLUMN_LIST := V_COLUMN_LIST || E',\n  CONSTRAINT ' || quote_ident(r.conname) || ' UNIQUE (' || r.uq_columns || ')';
    end loop;
    --
    return next 'CREATE TABLE ' || quote_ident(P_SCHEMA) || '.' || quote_ident(V_TABLE_NAME) || E' (\n  ' || V_COLUMN_LIST || E'\n)';
    --
    -- Reset for next table
    V_PK_COLUMNS := null;
    V_CONSTRAINT_NAME := null;
  end loop;
  --
  -- =============================================
  -- 4. INDEXES (non-constraint, non-primary-key)
  -- =============================================
  --
  for r in
    select i.indexrelid
      from pg_index i
      join pg_class c on c.oid = i.indrelid
      left join pg_constraint con on con.conindid = i.indexrelid
     where c.relnamespace = V_SCHEMA_OID
       and c.relkind = 'r'
       and con.oid is null
       and not i.indisprimary
     order by c.relname, i.indexrelid
  loop
    V_DDL_STATEMENT := pg_get_indexdef(r.indexrelid);
    -- Strip TABLESPACE clause
    V_DDL_STATEMENT := regexp_replace(V_DDL_STATEMENT, '\s+TABLESPACE\s+\S+', '', 'i');
    return next V_DDL_STATEMENT;
  end loop;
  --
  -- =============================================
  -- 5. CHECK CONSTRAINTS (ALTER TABLE ADD CONSTRAINT)
  -- =============================================
  --
  for r in
    select con.conname,
           c.relname as table_name,
           pg_get_constraintdef(con.oid) as constraint_def
      from pg_constraint con
      join pg_class c on c.oid = con.conrelid
     where con.connamespace = V_SCHEMA_OID
       and con.contype = 'c'
       and c.relkind = 'r'
     order by c.relname, con.conname
  loop
    return next 'ALTER TABLE ' || quote_ident(P_SCHEMA) || '.' || quote_ident(r.table_name) || ' ADD CONSTRAINT ' || quote_ident(r.conname) || ' ' || r.constraint_def;
  end loop;
  --
  -- =============================================
  -- 6. EXCLUSION CONSTRAINTS (ALTER TABLE ADD CONSTRAINT)
  -- =============================================
  --
  for r in
    select con.conname,
           c.relname as table_name,
           pg_get_constraintdef(con.oid) as constraint_def
      from pg_constraint con
      join pg_class c on c.oid = con.conrelid
     where con.connamespace = V_SCHEMA_OID
       and con.contype = 'x'
       and c.relkind = 'r'
     order by c.relname, con.conname
  loop
    return next 'ALTER TABLE ' || quote_ident(P_SCHEMA) || '.' || quote_ident(r.table_name) || ' ADD CONSTRAINT ' || quote_ident(r.conname) || ' ' || r.constraint_def;
  end loop;
  --
  -- =============================================
  -- 7. FOREIGN KEYS (ALTER TABLE ADD CONSTRAINT ... FOREIGN KEY)
  -- =============================================
  --
  for r in
    select con.conname,
           c.relname as table_name,
           pg_get_constraintdef(con.oid) as constraint_def
      from pg_constraint con
      join pg_class c on c.oid = con.conrelid
     where con.connamespace = V_SCHEMA_OID
       and con.contype = 'f'
     order by c.relname, con.conname
  loop
    return next 'ALTER TABLE ' || quote_ident(P_SCHEMA) || '.' || quote_ident(r.table_name) || ' ADD CONSTRAINT ' || quote_ident(r.conname) || ' ' || r.constraint_def;
  end loop;
  --
  -- =============================================
  -- 8. VIEWS (dependency-ordered where possible)
  -- =============================================
  --
  for r in
    with recursive view_deps as (
      select c.oid, c.relname, 0 as depth
        from pg_class c
       where c.relnamespace = V_SCHEMA_OID
         and c.relkind = 'v'
         and not exists (
           select 1
             from pg_depend d
             join pg_rewrite rw on rw.oid = d.objid
             join pg_class dep on dep.oid = d.refobjid
            where rw.ev_class = c.oid
              and dep.relnamespace = V_SCHEMA_OID
              and dep.relkind = 'v'
              and dep.oid <> c.oid
         )
      union all
      select c.oid, c.relname, vd.depth + 1
        from pg_class c
        join pg_depend d on d.refobjid = c.oid
        join pg_rewrite rw on rw.oid = d.objid
        join pg_class dep on dep.oid = rw.ev_class
        join view_deps vd on vd.oid = dep.oid
       where c.relnamespace = V_SCHEMA_OID
         and c.relkind = 'v'
         and c.oid <> dep.oid
    )
    select distinct on (v.oid) v.oid, v.relname, v.depth
      from view_deps v
     order by v.oid, v.depth desc
  loop
    V_DDL_STATEMENT := 'CREATE OR REPLACE VIEW ' || quote_ident(P_SCHEMA) || '.' || quote_ident(r.relname) || ' AS ' || pg_get_viewdef(r.oid, true);
    -- Remove trailing semicolons if present from pg_get_viewdef
    V_DDL_STATEMENT := rtrim(V_DDL_STATEMENT, ';');
    return next V_DDL_STATEMENT;
  end loop;
  --
  -- Catch any views not captured by dependency walk
  --
  for r in
    select c.oid, c.relname
      from pg_class c
     where c.relnamespace = V_SCHEMA_OID
       and c.relkind = 'v'
       and not exists (
         select 1 from (
           with recursive view_deps as (
             select c2.oid
               from pg_class c2
              where c2.relnamespace = V_SCHEMA_OID and c2.relkind = 'v'
                and not exists (
                  select 1 from pg_depend d2
                  join pg_rewrite rw2 on rw2.oid = d2.objid
                  join pg_class dep2 on dep2.oid = d2.refobjid
                  where rw2.ev_class = c2.oid and dep2.relnamespace = V_SCHEMA_OID and dep2.relkind = 'v' and dep2.oid <> c2.oid
                )
             union all
             select c2.oid
               from pg_class c2
               join pg_depend d2 on d2.refobjid = c2.oid
               join pg_rewrite rw2 on rw2.oid = d2.objid
               join pg_class dep2 on dep2.oid = rw2.ev_class
               join view_deps vd2 on vd2.oid = dep2.oid
              where c2.relnamespace = V_SCHEMA_OID and c2.relkind = 'v' and c2.oid <> dep2.oid
           )
           select oid from view_deps
         ) already_emitted
         where already_emitted.oid = c.oid
       )
     order by c.relname
  loop
    V_DDL_STATEMENT := 'CREATE OR REPLACE VIEW ' || quote_ident(P_SCHEMA) || '.' || quote_ident(r.relname) || ' AS ' || pg_get_viewdef(r.oid, true);
    V_DDL_STATEMENT := rtrim(V_DDL_STATEMENT, ';');
    return next V_DDL_STATEMENT;
  end loop;
  --
  -- =============================================
  -- 9. FUNCTIONS and PROCEDURES
  -- =============================================
  --
  for r in
    select p.oid
      from pg_proc p
     where p.pronamespace = V_SCHEMA_OID
       and p.prokind in ('f', 'p', 'a')
     order by p.proname, p.oid
  loop
    begin
      V_DDL_STATEMENT := pg_get_functiondef(r.oid);
      return next V_DDL_STATEMENT;
    exception
      when others then
        -- Some internal/aggregate functions may not support pg_get_functiondef
        null;
    end;
  end loop;
  --
  -- =============================================
  -- 10. TRIGGERS
  -- =============================================
  --
  for r in
    select t.oid as trigger_oid
      from pg_trigger t
      join pg_class c on c.oid = t.tgrelid
     where c.relnamespace = V_SCHEMA_OID
       and not t.tgisinternal
     order by c.relname, t.tgname
  loop
    return next pg_get_triggerdef(r.trigger_oid);
  end loop;
  --
end;
$$
LANGUAGE plpgsql;
--
