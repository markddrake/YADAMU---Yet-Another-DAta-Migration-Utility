create database yadamu;
\c yadamu
CREATE EXTENSION postgis;
SELECT PostGIS_version();
CREATE EXTENSION plpython3u;
CREATE EXTENSION file_fdw;
\q