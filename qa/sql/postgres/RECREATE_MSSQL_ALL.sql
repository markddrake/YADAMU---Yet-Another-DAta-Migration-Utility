--
select 'Northwind' || :'ID' "SCHEMA" \gset
--
\echo :SCHEMA
--
drop schema if exists :"SCHEMA" cascade;
--
create schema :"SCHEMA";
--
select 'Sales' || :'ID' "SCHEMA" \gset
--
\echo :SCHEMA
--
drop schema if exists :"SCHEMA" cascade;
--
create schema :"SCHEMA";
--
select 'Person' || :'ID' "SCHEMA" \gset
--
\echo :SCHEMA
--
drop schema if exists :"SCHEMA" cascade;
--
create schema :"SCHEMA";
--
select 'Production' || :'ID' "SCHEMA" \gset
--
\echo :SCHEMA
--
drop schema if exists :"SCHEMA" cascade;
--
create schema :"SCHEMA";
--
select 'Purchasing' || :'ID' "SCHEMA" \gset
--
\echo :SCHEMA
--
drop schema if exists :"SCHEMA" cascade;
--
create schema :"SCHEMA";
--
select 'HumanResources' || :'ID' "SCHEMA" \gset
--
\echo :SCHEMA
--
drop schema if exists :"SCHEMA" cascade;
--
create schema :"SCHEMA";
--
select 'AdventureWorksDW' || :'ID' "SCHEMA" \gset
--
\echo :SCHEMA
--
drop schema if exists :"SCHEMA" cascade;
--
create schema :"SCHEMA";
--
select 'WWI_Application' || :'ID' "SCHEMA" \gset
--
\echo :SCHEMA
--
drop schema if exists :"SCHEMA" cascade;
--
create schema :"SCHEMA";
--
select 'WWI_Sales' || :'ID' "SCHEMA" \gset
--
\echo :SCHEMA
--
drop schema if exists :"SCHEMA" cascade;
--
create schema :"SCHEMA";
--
select 'WWI_Purchasing' || :'ID' "SCHEMA" \gset
--
\echo :SCHEMA
--
drop schema if exists :"SCHEMA" cascade;
--
create schema :"SCHEMA";
--
select 'WWI_Warehouse' || :'ID' "SCHEMA" \gset
--
\echo :SCHEMA
--
drop schema if exists :"SCHEMA" cascade;
--
create schema :"SCHEMA";
--
select 'WWI_Dimension' || :'ID' "SCHEMA" \gset
--
\echo :SCHEMA
--
drop schema if exists :"SCHEMA" cascade;
--
create schema :"SCHEMA";
--
select 'WWI_Fact' || :'ID' "SCHEMA" \gset
--
\echo :SCHEMA
--
drop schema if exists :"SCHEMA" cascade;
--
create schema :"SCHEMA";
--
select 'WWI_Integration' || :'ID' "SCHEMA" \gset
--
\echo :SCHEMA
--
drop schema if exists :"SCHEMA" cascade;
--
create schema :"SCHEMA";
--
\quit