--
SET @SCHEMA = 'Northwind';
--
:r sql\SCHEMA_COMPARE.sql
--
SET @SCHEMA = 'Sales';
--
:r sql\SCHEMA_COMPARE.sql
--
SET @SCHEMA = 'Person';
--
:r sql\SCHEMA_COMPARE.sql
--
SET @SCHEMA = 'Production';
--
:r sql\SCHEMA_COMPARE.sql
--
SET @SCHEMA = 'Purchasing';
--
:r sql\SCHEMA_COMPARE.sql
--
SET @SCHEMA = 'HumanResources';
--
:r sql\SCHEMA_COMPARE.sql
--
SET @SCHEMA = 'DW';
--
:r sql\SCHEMA_COMPARE.sql
--
quit

