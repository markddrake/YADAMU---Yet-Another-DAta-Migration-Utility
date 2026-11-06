--
-- Version 23.26 (26ai) Specific Configuration
--
@@configure_CDB
--
@@configure_redo
--
-- 26ai Database Free and Enterprise Edition both report release 23.26. Free is capped at about 1.5 GB of SGA,
-- so it only gets a larger shared pool (configure_2326_free.sql); Enterprise Edition is sized like the other
-- versions (configure_memory.sql).
--
column EDITION_SCRIPT new_value EDITION_SCRIPT
select case when EDITION = 'FREE' then 'configure_2326_free' else 'configure_memory' end EDITION_SCRIPT
  from V$INSTANCE
/
@@&EDITION_SCRIPT
--
