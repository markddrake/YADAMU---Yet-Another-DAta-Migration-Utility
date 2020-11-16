create database sakila;
use sakila;
source testdata/sakila/sakila-schema.sql
source testdata/sakila/sakila-data.sql
create database jtest;
use jtest;
source testdata/jtest.audit.sql
exit
rm -rf setup
rm -rf testdata