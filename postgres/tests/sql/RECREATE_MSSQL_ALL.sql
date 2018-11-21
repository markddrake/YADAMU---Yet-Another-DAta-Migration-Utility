--
select 'Northwind' || :ID "SCHEMA" \gset
--
drop schema :"SCHEMA" cascade;
--
create schema :"SCHEMA";
--
select 'Sales' || :ID "SCHEMA" \gset
--
drop schema :"SCHEMA" cascade;
--
create schema :"SCHEMA";
--
select 'Person' || :ID "SCHEMA" \gset
--
drop schema :"SCHEMA" cascade;
--
create schema :"SCHEMA";
--
select 'Production' || :ID "SCHEMA" \gset
--
drop schema :"SCHEMA" cascade;
--
create schema :"SCHEMA";
--
select 'Purchasing' || :ID "SCHEMA" \gset
--
drop schema :"SCHEMA" cascade;
--
create schema :"SCHEMA";
--
select 'HumanResources' || :ID "SCHEMA" \gset
--
drop schema :"SCHEMA" cascade;
--
create schema :"SCHEMA";
--
select 'DW' || :ID "SCHEMA" \gset
--
drop schema :"SCHEMA" cascade;
--
create schema :"SCHEMA";
--
