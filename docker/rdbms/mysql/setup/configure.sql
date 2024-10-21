CREATE USER if not exists 'root'@'%' identified by  'oracle';
GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION;
ALTER USER 'root'@'%' IDENTIFIED WITH mysql_native_password BY 'oracle';
ALTER USER 'root'@'%' IDENTIFIED WITH caching_sha2_password BY 'oracle';
SET GLOBAL max_allowed_packet=1024*1024*1024;
create database if not exists  sakila;
use sakila;
source testdata/sakila/sakila-schema.sql
source testdata/sakila/sakila-data.sql
create database if not exists  jtest;
use jtest;
source testdata/jtest.audit.sql
exit
exit


alter user 'root'@'%'  