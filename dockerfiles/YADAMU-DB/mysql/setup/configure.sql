ALTER USER 'root'@'%' IDENTIFIED WITH mysql_native_password BY 'oracle';
SET GLOBAL max_allowed_packet=1024*1024*1024;
create database sakila;
use sakila;
source testdata/sakila/sakila-schema.sql
source testdata/sakila/sakila-data.sql
create database jtest;
use jtest;
source testdata/jtest.audit.sql
exit
