--
SET @SCHEMA = 'HR';
--
source qa/sql/mariadb//SCHEMA_COMPARE.sql
--
SET @SCHEMA = 'SH';
--
source qa/sql/mariadb//SCHEMA_COMPARE.sql
--
SET @SCHEMA = 'OE';
--
source qa/sql/mariadb//SCHEMA_COMPARE.sql
--
SET @SCHEMA = 'PM';
--
source qa/sql/mariadb//SCHEMA_COMPARE.sql
--
SET @SCHEMA = 'IX';
--
source qa/sql/mariadb//SCHEMA_COMPARE.sql
--
SET @SCHEMA = 'BI';
--
source qa/sql/mariadb//SCHEMA_COMPARE.sql
--
quit

