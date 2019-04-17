--
select  :SCHEMA || :ID "SCHEMA" \gset
--
\echo :SCHEMA
--
drop schema :"SCHEMA" cascade;
--
create schema :"SCHEMA";
--
\quit