-- Version 11.2.0 Specific Configuration
--
-- Disable JIT compiler.. It doesn't like the way /dev/shm is mounted in the container
--
ALTER SYSTEM SET JAVA_JIT_ENABLED = FALSE
/
--