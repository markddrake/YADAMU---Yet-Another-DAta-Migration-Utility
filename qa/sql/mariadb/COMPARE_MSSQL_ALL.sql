--
SET @SCHEMA = 'Northwind';
--
source qa/sql/mariadb//SCHEMA_COMPARE.sql
--
SET @SCHEMA = 'Sales';
--
source qa/sql/mariadb//SCHEMA_COMPARE.sql
--
SET @SCHEMA = 'Person';
--
source qa/sql/mariadb//SCHEMA_COMPARE.sql
--
SET @SCHEMA = 'Production';
--
source qa/sql/mariadb//SCHEMA_COMPARE.sql
--
SET @SCHEMA = 'Purchasing';
--
source qa/sql/mariadb//SCHEMA_COMPARE.sql
--
SET @SCHEMA = 'HumanResources';
--
source qa/sql/mariadb//SCHEMA_COMPARE.sql
--
SET @SCHEMA = 'AdventureWorksDW';
--
source qa/sql/mariadb//SCHEMA_COMPARE.sql
--
quit

