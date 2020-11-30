--
select :'SCHEMA' || :'ID' "SCHEMA" \gset
--
\echo :SCHEMA
--
drop schema if exists :"SCHEMA" cascade;
--
create schema :"SCHEMA";
--
\quit