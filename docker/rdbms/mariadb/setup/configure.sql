CREATE USER if not exists 'root'@'%' identified by  'oracle';
GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION;
SET GLOBAL max_allowed_packet=1024*1024*1024;
create database if not exists  sakila;
create database if not exists  jtest;
-- use sakila;
-- source testdata/sakila/sakila-schema.sql
-- source testdata/sakila/sakila-data.sql
-- use jtest;
-- source testdata/jtest.audit.sql
\quit