import YadamuConstants from '../../lib/yadamuConstants.js';

class CockroachStatementLibrary {
    
  static get SQL_CONFIGURE_CONNECTION()       { return SQL_CONFIGURE_CONNECTION }
  static get SQL_SYSTEM_INFORMATION()         { return SQL_SYSTEM_INFORMATION }
  static get SQL_GET_DLL_STATEMENTS()         { return SQL_GET_DLL_STATEMENTS }
  static get SQL_SCHEMA_INFORMATION()         { return SQL_SCHEMA_INFORMATION } 
  static get SQL_BEGIN_TRANSACTION()          { return SQL_BEGIN_TRANSACTION }
  static get SQL_COMMIT_TRANSACTION()         { return SQL_COMMIT_TRANSACTION }
  static get SQL_ROLLBACK_TRANSACTION()       { return SQL_ROLLBACK_TRANSACTION }
  static get SQL_CREATE_SAVE_POINT()          { return SQL_CREATE_SAVE_POINT }  
  static get SQL_RESTORE_SAVE_POINT()         { return SQL_RESTORE_SAVE_POINT }
  static get SQL_RELEASE_SAVE_POINT()         { return SQL_RELEASE_SAVE_POINT }
  static get SQL_POSTGIS_INFO()               { return SQL_POSTGIS_INFO }

}

export { CockroachStatementLibrary as default }

const SQL_CONFIGURE_CONNECTION = `set timezone to 'UTC'; SET extra_float_digits to 3; SET Intervalstyle = 'iso_8601'`

const SQL_SCHEMA_INFORMATION   = 
          `with "params" ("schema", "spatial_format") 
           as (
             values ($1::character varying, $2::character varying) 
           )
           select t.table_schema "TABLE_SCHEMA"
                 ,t.table_name "TABLE_NAME"
                 ,jsonb_agg(column_name order by ordinal_position) "COLUMN_NAME_ARRAY"
                 ,jsonb_agg(case 
                              when ((c.data_type = 'character') and (c.character_maximum_length is null)) then
                                c.udt_name 
                              when c.data_type = 'USER-DEFINED' then
                                c.udt_name 
                              when c.data_type = 'ARRAY' then
                                e.data_type || ' ARRAY'
                              when ((c.data_type = 'interval') and (c.interval_type is not null)) then
                                c.data_type || ' ' || c.interval_type
                              else 
                                c.data_type 
                            end 
                            order by ordinal_position
                           ) "DATA_TYPE_ARRAY"
                 ,jsonb_agg(case
                              when (c.numeric_precision is not null) and (c.numeric_scale is not null) then
                                jsonb_build_array(c.numeric_precision,c.numeric_scale)
                              when (c.numeric_precision is not null) then 
                                jsonb_build_array(c.numeric_precision)
                              when (c.character_maximum_length is not null) then 
                                jsonb_build_array(c.character_maximum_length)
                              when (c.datetime_precision is not null) then 
                                jsonb_build_array(c.datetime_precision)
                              else
                                jsonb_build_array()
                            end
                            order by ordinal_position
                          ) "SIZE_CONSTRAINT_ARRAY"
                 ,string_agg(case 
                               when c.data_type in ('timestamp without time zone','timestamp with time zone') then
                                 'replace(replace(replace(("' || COLUMN_NAME ||'" at time zone ''UTC'')::character varying,'' '',''T''),''+00:00'',''Z''),''+00'',''Z'')  "' || COLUMN_NAME || '"'
                               when c.data_type in ('date') then
                                 'replace(replace(replace(("' || COLUMN_NAME ||'"::timestamp at time zone ''UTC'')::character varying,'' '',''T''),''+00:00'',''Z''),''+00'',''Z'')  "' || COLUMN_NAME || '"'
                               when c.data_type in ('time without time zone','time with time zone') then
                                 'replace(replace(replace(((''epoch''::date + "' || COLUMN_NAME || '")::timestamp at time zone ''UTC'')::character varying,'' '',''T''),''+00:00'',''Z''),''+00'',''Z'')  "' || COLUMN_NAME || '"'
                               when c.data_type like 'interval%' then
                                 '"' || COLUMN_NAME ||'"::varchar "' || COLUMN_NAME || '"'
                               when (c.data_type in ('real')) then 
                                 '"' || column_name || '"::double precision "' || COLUMN_NAME || '"'
                               when (c.data_type in ('decimal','numeric')) then 
                                 -- Workaround for issue with 0 being rendered in scientific notation
                                 'case when "' || column_name || '" = 0 then ''0''::numeric else  "' || column_name || '" end "' || COLUMN_NAME || '"'
                               when (c.data_type in ('geometry'))  then
                                 case 
                                   when p."spatial_format" = 'WKB' then
                                     'ST_AsBinary("' || column_name || '") "' || COLUMN_NAME || '"'
                                   when p."spatial_format" = 'WKT' then
                                     'ST_AsText("' || column_name || '",18) "' || COLUMN_NAME || '"'
                                   when p."spatial_format" = 'EWKB' then
                                     'ST_AsEWKB("' || column_name || '") "' || COLUMN_NAME || '"'
                                   when p."spatial_format" = 'EWKT' then
                                     'ST_AsEWKT("' || column_name || '") "' || COLUMN_NAME || '"'
                                   when p."spatial_format" = 'GeoJSON' then
                                     'ST_AsGeoJSON("' || column_name || '") "' || COLUMN_NAME || '"'
                                 end
                               when (c.data_type in ('geography')) then
                                 case 
                                   when p."spatial_format" = 'WKB' then
                                     'ST_AsBinary("' || column_name || '") "' || COLUMN_NAME || '"'
                                   when p."spatial_format" = 'WKT' then
                                     'ST_AsText("' || column_name || '",18) "' || COLUMN_NAME || '"'
                                   when p."spatial_format" = 'EWKB' then
                                     'ST_AsBinary("' || column_name || '") "' || COLUMN_NAME || '"'
                                   when p."spatial_format" = 'EWKT' then
                                     'ST_AsText("' || column_name || '") "' || COLUMN_NAME || '"'
                                   when p."spatial_format" = 'GeoJSON' then
                                     'ST_AsGeoJSON("' || column_name || '") "' || COLUMN_NAME || '"'
                                 end
                               else
                                 '"' || column_name || '"'
                             end,
                             ',' order by ordinal_position
                               ) "CLIENT_SELECT_LIST"
            from "params" p, information_schema.columns c
            left join information_schema.tables t 
              on ((t.table_schema, t.table_name) = (c.table_schema, c.table_name))
            left join information_schema.element_types e
              on ((c.table_catalog, c.table_schema, c.table_name, 'TABLE', c.dtd_identifier) = (e.object_catalog, e.object_schema, e.object_name, e.object_type, e.collection_type_identifier))
           where t.table_type = 'BASE TABLE' %ROWID_FILTER%
             and t.table_schema =  p."schema"
        group by t.table_schema, t.table_name`;
 
const SQL_SYSTEM_INFORMATION   = `select current_database() database_name,current_user, session_user, current_setting('server_version_num') database_version, right(cast(current_timestamp as character varying),6) timezone`;

const SQL_BEGIN_TRANSACTION    = `begin transaction`

const SQL_COMMIT_TRANSACTION   = `commit transaction`

const SQL_ROLLBACK_TRANSACTION = `rollback transaction`

const SQL_CREATE_SAVE_POINT    = `savepoint "${YadamuConstants.SAVE_POINT_NAME}"`;

const SQL_RESTORE_SAVE_POINT   = `rollback to savepoint "${YadamuConstants.SAVE_POINT_NAME}"`;

const SQL_RELEASE_SAVE_POINT   = `release savepoint "${YadamuConstants.SAVE_POINT_NAME}"`;

const SQL_POSTGIS_INFO         = `select  PostGIS_version() "POSTGIS"`

