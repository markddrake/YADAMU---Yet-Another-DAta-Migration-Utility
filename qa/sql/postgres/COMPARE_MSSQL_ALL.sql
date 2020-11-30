--
\set ECHO errors
--
\set SCHEMA Northwind
--
\include qa/sql/postgres//SCHEMA_COMPARE.sql
--
\set SCHEMA Sales
--
\include qa/sql/postgres/SCHEMA_COMPARE.sql
--
\set SCHEMA Person
--
\include qa/sql/postgres/SCHEMA_COMPARE.sql
--
\set SCHEMA Production
--
\include qa/sql/postgres/SCHEMA_COMPARE.sql
--
\set SCHEMA Purchasing
--
\include qa/sql/postgres/SCHEMA_COMPARE.sql
--
\set SCHEMA HumanResources
--
\include qa/sql/postgres/SCHEMA_COMPARE.sql
--
\set SCHEMA AdventureWorksDW
--
\include qa/sql/postgres/SCHEMA_COMPARE.sql
--
\quit

