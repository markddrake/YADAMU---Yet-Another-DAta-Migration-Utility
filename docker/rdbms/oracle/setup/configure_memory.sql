--
-- Size the instance to its container, not to the host.
--
-- DBCA sizes memory from the host's RAM (memoryPercentage=85 in dbca.rsp), but the regression
-- containers are capped at 16 GB (mem_limit). On a 125 GB host that gave an 80 GB SGA with a
-- 69 GB buffer cache and a 26.6 GB PGA target, so the shared memory was pushed out to swap once
-- the container reached its limit, and "cached" blocks were read back from disk. The sizes are
-- fixed, so every host runs the same configuration. They take effect on restart: configure.sh
-- restarts the database when the restartRequired marker exists.
--
alter system set sga_max_size=10G scope=spfile
/
alter system set sga_target=10G scope=spfile
/
alter system set pga_aggregate_target=2G scope=spfile
/
host touch &STAGE_LOCATION/restartRequired
--
