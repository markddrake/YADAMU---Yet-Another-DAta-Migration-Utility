--
-- 26ai Database Free (called from configure_2326.sql)
--
-- Oracle Database Free caps the SGA at about 1.5 GB, and by default only 560 MB of it is shared pool.
-- DBMS_METADATA's KU$ cursors then age out between uses, so every DDL export hard parses about
-- 200 statements. Move memory from the buffer cache to the
-- shared pool. The sizes are minimums and only take effect on restart; configure.sh restarts the
-- database when the restartRequired marker exists.
--
alter system set shared_pool_size=1024M scope=spfile
/
alter system set db_cache_size=448M scope=spfile
/
host touch &STAGE_LOCATION/restartRequired
--
