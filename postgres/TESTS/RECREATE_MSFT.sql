--
select 'MSFT' || :ID "SCHEMA" \gset
--
drop schema :"SCHEMA" cascade;
--
create schema :"SCHEMA";
--