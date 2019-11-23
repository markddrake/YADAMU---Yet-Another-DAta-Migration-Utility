--
select 'HR' || :'ID' "SCHEMA" \gset
--
\echo :SCHEMA
--
drop schema :"SCHEMA" cascade;
--
create schema :"SCHEMA";
--
select 'SH' || :'ID' "SCHEMA" \gset
--
\echo :SCHEMA
--
drop schema :"SCHEMA" cascade;
--
create schema :"SCHEMA";
--
select 'OE' || :'ID' "SCHEMA" \gset
--
\echo :SCHEMA
--
drop schema :"SCHEMA" cascade;
--
create schema :"SCHEMA";
--
select 'PM' || :'ID' "SCHEMA" \gset
--
\echo :SCHEMA
--
drop schema :"SCHEMA" cascade;
--
create schema :"SCHEMA";
--
select 'IX' || :'ID' "SCHEMA" \gset
--
\echo :SCHEMA
--
drop schema :"SCHEMA" cascade;
--
create schema :"SCHEMA";
--
select 'BI' || :'ID' "SCHEMA" \gset
--
\echo :SCHEMA
--
drop schema :"SCHEMA" cascade;
--
create schema :"SCHEMA";
--
\quit
