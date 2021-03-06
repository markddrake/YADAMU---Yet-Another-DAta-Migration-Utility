drop schema t_postgres cascade;
--
create schema t_postgres;
--
--  8.1. Numeric Types, 8.2. Monetary Types
--
create table t_postgres.numeric_types(
  smallint_col                  smallint,
  integer_col                   integer,
  bigint_col                    bigint,
  decimal_col                   decimal,
  numeric_col                   numeric,
  real_col                      real,
  double_precision_col          double precision,
  money_col                     money,
  small_serial_col              smallserial,
  serial_col                    serial,
  bigserial_col                 bigserial
);
--
insert into t_postgres.numeric_types 
values(
  -32768,
  -2147483648,
  -9223372036854775808,
  12356789.123456789,
  12356789.123456789,
  1.17549e-38,
  2.22507e-308,
  -92233720368547758.08,
  DEFAULT,
  DEFAULT,
  DEFAULT
);
--
insert into t_postgres.numeric_types 
values(
  32767, 
  2147483647,
  9223372036854775807,
  12356789.123456789,
  12356789.123456789,
  3.4028235e+38,
  1.79769e308,
  92233720368547758.07,
  DEFAULT,
  DEFAULT,
  DEFAULT
);
--
insert into t_postgres.numeric_types 
values(
  NULL, 
  NULL,
  NULL,
  NULL,
  NULL,
  'Infinity',
  'Infinity',
  NULL,
  DEFAULT,
  DEFAULT,
  DEFAULT
);
--
insert into t_postgres.numeric_types 
values(
  NULL, 
  NULL,
  NULL,
  NULL,
  NULL,
  '-Infinity',
  '-Infinity',
  NULL,
  DEFAULT,
  DEFAULT,
  DEFAULT
);
--
insert into t_postgres.numeric_types 
values(
  NULL, 
  NULL,
  NULL,
  NULL,
  NULL,
  'NaN',
  'NaN',
  NULL,
  DEFAULT,
  DEFAULT,
  DEFAULT
);
--
-- 8.3. Character Types
--
create table t_postgres.character_types(
  character_col              character,
  character_varying_col      character varying,
  character_max_col          character(10485760),
  character_varying_max_col  character varying(10485760),
  character_4000_col         character(4000),
  character_varying_4000_col character varying(4000),
  character_64K_col          character(65535),
  character_varying_64K_col  character varying(65535),
  text_col                   text
);
--
insert into  t_postgres.character_types
values (
 'A',
 REPEAT('ABCD-',100),
 REPEAT('ABCD-',(10485760/5)),
 REPEAT('ABCD-',(10485760/5)), 
 REPEAT('ABCD',1000),
 REPEAT('ABCD',1000),
 REPEAT('X',65535),
 REPEAT('X',65535),
 REPEAT('ABCD-',(10485760/5))
);
--
-- 8.5. Date/Time Types
--
create table t_postgres.temporal_types (
  timestamp_col              timestamp
 ,timestamptz_col            timestamptz
 ,date_col                   date
 ,time_col                   time 
 ,timetz_col                 timetz
 ,interval_col               interval
);
--
insert into t_postgres.temporal_types values ( 'epoch'::timestamp,  'epoch'::timestamptz,  'epoch'::date,  'allballs'::time,  'epoch'::timestamptz::timetz,  '1y');
--
insert into t_postgres.temporal_types values ( 'now'::timestamp,  'now'::timestamptz,  'now'::date,  'now'::time,  'now'::timestamptz::timetz,  '365d');
--
insert into t_postgres.temporal_types values ( 'today'::timestamp,  'today'::timestamptz,  'today'::date,  'today'::timestamp::time,  'today'::timestamptz::timetz,  '1d');
--
insert into t_postgres.temporal_types values ( 'tomorrow'::timestamp,  'tomorrow'::timestamptz,  'tomorrow'::date,  'tomorrow'::timestamp::time,  'tomorrow'::timestamptz::timetz,  '24h');
--
insert into t_postgres.temporal_types values ( 'yesterday'::timestamp,  'yesterday'::timestamptz,  'yesterday'::date,  'yesterday'::timestamp::time,  'yesterday'::timestamptz::timetz,  '1h');
--
-- insert into t_postgres.temporal_types values ( 'epoch'::timestamp,  'epoch'::timestamptz at time zone 'PST',  'epoch'::date,  'allballs'::time,  'epoch'::timestamptz::timetz at time zone 'PST',  '1m');
--
-- insert into t_postgres.temporal_types values ( 'now'::timestamp,  'now'::timestamptz  at time zone 'PST',  'now'::date,  'now'::timestamp::time,  'now'::timestamptz::timetz at time zone 'PST',  '60s');
--
-- insert into t_postgres.temporal_types values ( 'today'::timestamp,  'today'::timestamptz  at time zone 'PST',  'today'::date,  'today'::timestamp::time,  'today'::timestamptz::timetz  at time zone 'PST',  '1s');
--
-- insert into t_postgres.temporal_types values ( 'tomorrow'::timestamp,  'tomorrow'::timestamptz  at time zone 'PST',  'tomorrow'::date,  'tomorrow'::timestamp::time,  'tomorrow'::timestamptz::timetz  at time zone 'PST',  '1y');
--
-- insert into t_postgres.temporal_types values ( 'yesterday'::timestamp,  'yesterday'::timestamptz  at time zone 'PST',  'yesterday'::date,  'yesterday'::timestamp::time,  'yesterday'::timestamptz::timetz  at time zone 'PST',  '1y');
--
-- insert into t_postgres.temporal_types values ( 'Infinity'::timestamp,  'Infinity'::timestamptz,  'Infinity'::date,  'allballs'::time,  'Infinity'::timestamptz::timetz,  '1y');
--
-- insert into t_postgres.temporal_types values ( '-Infinity'::timestamp,  '-Infinity'::timestamptz,  '-Infinity'::date,  'allballs'::time,  '-Infinity'::timestamptz::timetz,  '1y');
--
-- insert into t_postgres.temporal_types values ( '4713-01-01 00:00:00 BC'::timestamp,  '4713-01-01 00:00:00 BC'::timestamptz,  '4713-01-01'::date,  '00:00:00'::time,  '00:00:00+1559'::timetz at time zone 'UTC',  '-178000000y');
--
-- insert into t_postgres.temporal_types values ( '294276-12-31 00:00:00 AD'::timestamp,  '294276-12-31 00:00:00 AD'::timestamptz,  '5874897-01-01'::date,  '24:00:00'::time,  '24:00:00-1559'::timetz at time zone 'UTC',  '178000000y');
--
--
-- 8.8. Geometric Types
--
create table t_postgres.geometric_types (
  point_data              point,
  line_data               line,
  line_segment_data       lseg,
  box_data                box,
  path_data               path,
  polygon_data            polygon,
  circle_data             circle
);
--
insert into t_postgres.geometric_types 
values(
  POINT(1,2),
  LINE '{2,4,5}',
  LSEG(POINT(2,3),POINT(5,2)),
  BOX(POINT(1,2),POINT(5,7)),
  path '((-1,0),(1,0))',
  polygon '((0,0),(1,3),(2,0))',
  circle '<2,1,3>'
);
--
-- 8.9. Network Address Types
--
create table t_postgres.network_types (
  cidr_col                   cidr 
 ,inet_col                   inet 
 ,macaddr_col                macaddr 
 ,macaddr8_col               macaddr8
);
--
insert into t_postgres.network_types
values (
  '2001:4f8:3:ba:​2e0:81ff:fe22:d1f1/128',
  '2001:4f8:3:ba::/64',
  '0800-2b01-0203',
  '08002b01:02030405'
);
--
insert into t_postgres.network_types
values (
  '192.168.100.128/25',
  '192.168.0./24',
  '08-00-2b-01-02-03',
  '08-00-2b-01-02-03-04-05'
);
--
-- 
-- 8.13. XML Type
-- 8.14. JSON Types
--
create table t_postgres.document_types(
  xml_col                    xml 
 ,json_col                   json 
 ,jsonb_col                  jsonb 
);
--
insert into t_postgres.document_types
values (
  '<XML/>',
  '{}',
  '{}'
);
--
insert into t_postgres.document_types
values (
  '<XML></XML>',
  '[]',
  '[]'
);
--
-- 8.12. UUID Type
--
create table t_postgres.id_types(
  uuid_col                   uuid 
);
--
insert into t_postgres.id_types
values (
  'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11'
);
--
-- 8.4. Binary Data Types
-- 8.6. Boolean Type
-- 8.10. Bit String Types
--
create table t_postgres.binary_types(
     bytea_col                  bytea 
    ,bool_col                   bool 
    ,bit_col                    bit 
    ,bit_varying_col            bit varying 
    ,bit_8_col                  bit(8) 
    ,bit_varying_64_col         bit varying(64)
);
--
insert into  t_postgres.binary_types
values (
  decode('0000000000','hex'),
  false,
  0::bit,
  B'000',
  x'0'::bit(8),
  x'1'::bit varying(64)
);
insert into  t_postgres.binary_types
values (
  decode('FFFFFFF','hex'),
  true,
  1::bit,
  B'111',
  x'FF'::bit(8),
  x'F'::bit varying(64)
);
insert into  t_postgres.binary_types
values (
  null,
  '0',
  null,
  null,
  null,
  null
);
insert into  t_postgres.binary_types
values (
  null,
  '1',
  null,
  null,
  null,
  null
);
insert into  t_postgres.binary_types
values (
  null,
  'no',
  null,
  null,
  null,
  null
);
insert into  t_postgres.binary_types
values (
  null,
  'yes',
  null,
  null,
  null,
  null
);
insert into  t_postgres.binary_types
values (
  null,
  'off',
  null,
  null,
  null,
  null
);
insert into  t_postgres.binary_types
values (
  null,
  'on',
  null,
  null,
  null,
  null
);
--
-- 8.11. Text Search Types
--
create table t_postgres.ts_search_types (
  tsvector_col               tsvector 
 ,tsquery_col                tsquery 
);
insert into t_postgres.ts_search_types
values(
  'a fat cat sat on a mat and ate a fat rat'::tsvector,
  'fat & (rat | cat)'::tsquery
);
select YADAMU_AsJSON(tsvector_col) from t_postgres.ts_search_types;
select YADAMU_AsTSVECTOR(YADAMU_AsJSON(tsvector_col)) from t_postgres.ts_search_types;
--
-- 8.17. Range Types
--
create table t_postgres.range_types (
  int4range_col              int4range 
 ,int8range_col              int8range 
 ,numrange_col               numrange 
 ,tsrange_col                tsrange 
 ,tstzrange_col              tstzrange 
 ,daterange_col              daterange 
);
insert into t_postgres.range_types
values (
  '[1,4]'::int4range,
  '[1,8]'::int8range,
  '[-5,5]'::numrange,
  ('[' || now() - '5h'::interval || ',' || now() + '5h'::interval || ']')::tsrange,
  ('[' || now() - '5h'::interval || ',' || now() + '5h'::interval || ']')::tstzrange,
  ('[' || now() - '5d'::interval || ',' || now() + '5d'::interval || ']')::daterange
);
insert into t_postgres.range_types
values (
  '(1,4)'::int4range,
  '(1,8)'::int8range,
  '(-5,5)'::numrange,
  ('(' || now() - '5h'::interval || ',' || now() + '5h'::interval || ')')::tsrange,
  ('(' || now() - '5h'::interval || ',' || now() + '5h'::interval || ')')::tstzrange,
  ('(' || now() - '5d'::interval || ',' || now() + '5d'::interval || ')')::daterange
);
select YADAMU_AsJSON(int4range_col),
       YADAMU_AsJSON(int8range_col),
	   YADAMU_AsJSON(numrange_col),
	   YADAMU_AsJSON(tsrange_col),
	   YADAMU_AsJSON(tstzrange_col),
	   YADAMU_AsJSON(daterange_col)
  from t_postgres.range_types;
--
select YADAMU_AsRange(YADAMU_AsJSON(int4range_col))::int4range,
       YADAMU_AsRange(YADAMU_AsJSON(int8range_col))::int8range,
	   YADAMU_AsRange(YADAMU_AsJSON(numrange_col))::numrange,
	   YADAMU_AsRange(YADAMU_AsJSON(tsrange_col))::tsrange,
	   YADAMU_AsRange(YADAMU_AsJSON(tstzrange_col))::tstzrange,
	   YADAMU_AsRange(YADAMU_AsJSON(daterange_col))::daterange
  from t_postgres.range_types;
--
-- 8.19. Object Identifier Types
--
create table t_postgres.object_id_types (
  oid_col                    oid 
 ,regcollation_col           regcollation
 ,regclass_col               regclass 
 ,regconfig_col              regconfig 
 ,regdictionary_col          regdictionary 
 ,regnamespace_col           regnamespace 
 ,regoper_col                regoper 
 ,regoperator_col            regoperator 
 ,regproc_col                regproc 
 ,regprocedure_col           regprocedure
 ,regrole_col                regrole 
 ,regtype_col                regtype 
);
insert into t_postgres.object_id_types 
values (
  1,2,3,4,5,6,7,9,9,10,11,12
);
/*
--
-- 8.21. Pseudo-Types
--
--  ,any_col                    "any"              -- ERROR:  column "any_col" has pseudo-type any
--  ,anyarray_col               anyarray           -- ERROR:  column "anyarray_col" has pseudo-type anyarray
--  ,anyelement_col             anyelement         -- ERROR:  column "anyelement_col" has pseudo-type anyelement
--  ,anynonarray_col            anynonarray        -- ERROR:  column "anynonarray_col" has pseudo-type anynonarray
--  ,anyenum_col                anyenum            -- ERROR:  column "anyenum_col" has pseudo-type anyenum
--  ,anyrange_col               anyrange           -- ERROR:  column "anyrange_col" has pseudo-type anyrange
--  ,internal_col               internal           -- ERROR:  column "internal_col" has pseudo-type internal
--  ,cstring_col                cstring            -- ERROR:  column "cstring_col" has pseudo-type cstring
--  ,language_handler_col       language_handler   -- ERROR:  column "language_handler_col" has pseudo-type language_handler
--  ,fdw_handler_col            fdw_handler        -- ERROR:  column "fdw_handler_col" has pseudo-type fdw_handler
--  ,tsm_handler_col            tsm_handler        -- ERROR:  column "tsm_handler_col" has pseudo-type tsm_handler
--  ,record_col                 record             -- ERROR:  column "record_col" has pseudo-type record
--  ,trigger_col                trigger            -- ERROR:  column "trigger_col" has pseudo-type trigger
--  ,event_trigger_col          event_trigger      -- ERROR:  column "event_trigger_col" has pseudo-type event_trigger
--  ,void_col                   void               -- ERROR:  column "void_col" has pseudo-type void
--  ,unknown_col                unknown            -- ERROR:  column "unknown_col" has pseudo-type unknown
--
*/
--
--  Internal Types (Character)
--
create table t_postgres.internal_types(
  -- char_col                   "char",
  name_col                   name,
  bpchar_col                 bpchar,
  bpchar_64_col              bpchar(64)
);
--
insert into  t_postgres.internal_types
values (
 -- 'Z',
 REPEAT('ABCD',16),
 REPEAT('ABCD',32),
 REPEAT('ABCD',12)
);
--
-- Undocumented Types
--
create table t_postgres.undocumented_types (
  aclitem_col                aclitem 
 ,refcursor_col              refcursor 
 ,tid_col                    tid 
 ,xid_col                    xid 
 ,cid_col                    cid 
 ,txid_snapshot_col          txid_snapshot 
 ,gtsvector_col              gtsvector 
/*
 ,int2vector_col             int2vector         -- SMALLINT ARRAY
 ,oidvector_col              oidvector          -- OID ARRAY    
 ,opaque_col                 opaque             -- ERROR:  type "opaque" does not exist
 ,smgr_col                   smgr               -- ERROR:  type "smgr" does not exist
 ,abstime_col                abstime            -- ERROR:  type "abstime" does not exist
 ,reltime_col                reltime            -- ERROR:  type "reltime" does not exist
 ,tinterval_col              tinterval          -- ERROR:  type "tinterval" does not exist
*/
);
--
\q
create table t_postgres.numeric_arrays
  smallint_col                  smallint[],
  integer_col                   integer[],
  bigint_col                    bigint[],
  decimal_col                   decimal[],
  numeric_col                   numeric[],
  real_col                      real[],
  double_precision_col          double precision[],
  money_col                     money[],
  small_serial_col              smallserial[],
  serial_col                    serial[],
  bigserial_col                 bigserial[]
);
--
create table t_postgres.character_arrays(
  character_col              character[],
  character_varying_col      character varying[],
  character_max_col          character(10485760)[],
  character_varying_max_col  character varying(10485760)[],
  character_4000_col         character(4000)[],
  character_varying_4000_col character varying(4000)[],
  character_64K_col          character(65535)[],
  character_varying_64K_col  character varying(65535)[],
  text_col                   text[],
  char_col                   "char"[],
  name_col                   name[]
 ,bpchar_col                 bpchar[] 
 ,bpchar_64_col              bpchar[] 
);
create table t_postgres.temporal_arrays (
  timestamp_col              timestamp[]
 ,timestamptz_col            timestamptz[]
 ,date_col                   date[]
 ,time_col                   time[] 
 ,timetz_col                 timetz[]
 ,interval_col               interval[]
);
--
create table t_postgres.geometric_arrays (
  point_data              point[],
  line_data               line[],
  line_segment_data       lseg[],
  box_data                box[],
  path_data               path[],
  polygon_data            polygon[],
  circle_data             circle[]
);
--
-- 8.9. Network Address Types
--
create table t_postgres.network_arrays (
  cidr_col                   cidr[] 
 ,inet_col                   inet[] 
 ,macaddr_col                macaddr[] 
 ,macaddr8_col               macaddr8[]
);
-- 
-- 8.13. XML Type
-- 8.14. JSON Types
--
create table t_postgres.document_arrays(
  xml_col                    xml[] 
 ,json_col                   json[] 
 ,jsonb_col                  jsonb[] 
);

--
-- 8.12. UUID Type
--
create table t_postgres.id_arrays(
  uuid_col                   uuid[]
);
--
-- 8.4. Binary Data Types
-- 8.6. Boolean Type
-- 8.10. Bit String Types
--
create table t_postgres.binary_data_arrays(
     bytea_col                  bytea[] 
    ,bool_col                   bool[] 
    ,bit_col                    bit[] 
    ,bit_varying_col            bit varying[] 
    ,bit_8_col                  bit(8)[]
    ,bit_varying_64_col         bit varying(64)[]
);
--
-- 8.11. Text Search Types
--
create table t_postgres.ts_search_arrays (
  tsvector_col               tsvector[] 
 ,tsquery_col                tsquery[] 
);
--
-- 8.17. Range Types
--
create table t_postgres.range_arrays (
    ,int4range_col              int4range[] 
    ,int8range_col              int8range[] 
    ,numrange_col               numrange[] 
    ,tsrange_col                tsrange[] 
    ,tstzrange_col              tstzrange[] 
    ,daterange_col              daterange[] 
);
--
-- 8.19. Object Identifier Types
--
create table t_postgres.object_id_arrays (
  oid_col                    oid[]
 ,regcollation_col           regcollation[]
 ,regclass_col               regclass[]
 ,regconfig_col              regconfig[]
 ,regdictionary_col          regdictionary[] 
 ,regnamespace_col           regnamespace[] 
 ,regoper_col                regoper[] 
 ,regoperator_col            regoperator[]
 ,regproc_col                regproc[] 
 ,regprocedure_col           regprocedure[] 
 ,regrole_col                regrole[] 
 ,regtype_col                regtype[] 
);
/*
--
-- 8.21. Pseudo-Types
--
--  ,any_col                    "any"              -- ERROR:  column "any_col" has pseudo-type any
--  ,anyarray_col               anyarray           -- ERROR:  column "anyarray_col" has pseudo-type anyarray
--  ,anyelement_col             anyelement         -- ERROR:  column "anyelement_col" has pseudo-type anyelement
--  ,anynonarray_col            anynonarray        -- ERROR:  column "anynonarray_col" has pseudo-type anynonarray
--  ,anyenum_col                anyenum            -- ERROR:  column "anyenum_col" has pseudo-type anyenum
--  ,anyrange_col               anyrange           -- ERROR:  column "anyrange_col" has pseudo-type anyrange
--  ,internal_col               internal           -- ERROR:  column "internal_col" has pseudo-type internal
--  ,cstring_col                cstring            -- ERROR:  column "cstring_col" has pseudo-type cstring
--  ,language_handler_col       language_handler   -- ERROR:  column "language_handler_col" has pseudo-type language_handler
--  ,fdw_handler_col            fdw_handler        -- ERROR:  column "fdw_handler_col" has pseudo-type fdw_handler
--  ,tsm_handler_col            tsm_handler        -- ERROR:  column "tsm_handler_col" has pseudo-type tsm_handler
--  ,record_col                 record             -- ERROR:  column "record_col" has pseudo-type record
--  ,trigger_col                trigger            -- ERROR:  column "trigger_col" has pseudo-type trigger
--  ,event_trigger_col          event_trigger      -- ERROR:  column "event_trigger_col" has pseudo-type event_trigger
--  ,void_col                   void               -- ERROR:  column "void_col" has pseudo-type void
--  ,unknown_col                unknown            -- ERROR:  column "unknown_col" has pseudo-type unknown
*/
--
-- Undocumented Types
--
create table t_postgres.undocumented_arrays (
  aclitem_col                aclitem[] 
 ,refcursor_col              refcursor[] 
 ,int2vector_col             int2vector[] 
 ,tid_col                    tid[] 
 ,xid_col                    xid[] 
 ,cid_col                    cid[] 
 ,txid_snapshot_col          txid_snapshot[] 
 ,oidvector_col              oidvector[] 
 ,gtsvector_col              gtsvector[] 
 ,opaque_col                 opaque[]             -- ERROR:  type "opaque" does not exist
 ,smgr_col                   smgr[]               -- ERROR:  type "smgr" does not exist
 ,abstime_col                abstime[]            -- ERROR:  type "abstime" does not exist
 ,reltime_col                reltime[]            -- ERROR:  type "reltime" does not exist
 ,tinterval_col              tinterval[]          -- ERROR:  type "tinterval" does not exist
);
--
\q
--
insert into t_postgres.standard_array_types values(Array['Low','High'],Array['Values','Values'],
                                                   Array[-32768,32767],Array[-2147483648,+2147483647],Array[-9223372036854775808,9223372036854775807],Array[1.17549e-38,3.4028235e+38],Array[2.22507e-308,1.79769e308],Array[-1,1],Array[24.99,24.99],
                                                   Array[now(),now()],Array[now(),now()],Array[now(),now()],Array[now(),now()],Array[now(),now()],Array['24h'::interval,'1s'::interval], 
												   Array[false,true], Array[0::bit,1::bit],Array[B'000',B'111'],Array[decode('0000000000','hex'),decode('FFFFFFFF','hex')],
												   Array['{}'::json,'[]'::json], Array['{}'::jsonb,'[]'::jsonb], Array['<XML/>'::xml,'<XML></XML>'::xml]);
--
insert into t_postgres.spatial_array_types values (ARRAY[POINT(1,2),POINT(1,2)],ARRAY[LINE '{2,4,5}',LINE '{2,4,5}'],ARRAY[circle '<2,1,3>',circle '<2,1,3>'],ARRAY[LSEG(POINT(2,3),POINT(5,2)),LSEG(POINT(2,3),POINT(5,2))],
                                                   ARRAY[path '((-1,0),(1,0))',path '((-1,0),(1,0))'],ARRAY[BOX(POINT(1,2),POINT(5,7)),BOX(POINT(1,2),POINT(5,7))],ARRAY[polygon '((0,0),(1,3),(2,0))',polygon '((0,0),(1,3),(2,0))']);
--

select (array_to_tsvector('{fat,cat,rat}'::text[])));


