--
-- Redo: four 2 GB groups in place of the small ones DBCA creates (3 x 200 MB; 11.2.0.4 used to resize them to 250 MB).
--
-- With the small groups every large copy into Oracle is redo bound from PARALLEL 1 ("log file switch (checkpoint
-- incomplete)"). In the worker-threads scaling sweep a 16 table copy into 19c took 673 s at PARALLEL 16 with
-- 3 x 200 MB groups and 148 s with 4 x 2 GB groups.
--
-- The new groups go in the directory of the existing members. Each old group is dropped once it is INACTIVE:
-- the current one is switched out, then a checkpoint completes. A dropped group's file is left on disk unless it is
-- Oracle managed. Running the script again adds and drops nothing. Called from the version specific scripts, after
-- configure_CDB.sql has moved the session to CDB$ROOT where there is one.
--
declare
  C_GROUPS    constant pls_integer := 4;
  C_BYTES     constant number      := 2147483648; -- 2 GB
  V_DIRECTORY varchar2(513);
  V_GROUP     pls_integer;
  V_NEEDED    pls_integer;
  V_STATUS    varchar2(16);
begin
  select count(*) into V_NEEDED from V$LOG where BYTES >= C_BYTES;
  V_NEEDED := C_GROUPS - V_NEEDED;
  if (V_NEEDED > 0) then
    select substr(MEMBER, 1, instr(MEMBER, '/', -1)) into V_DIRECTORY from V$LOGFILE where ROWNUM = 1;
    select max(GROUP#) into V_GROUP from V$LOG;
    for i in 1 .. V_NEEDED loop
      V_GROUP := V_GROUP + 1;
      execute immediate 'ALTER DATABASE ADD LOGFILE THREAD 1 GROUP ' || V_GROUP || ' (''' || V_DIRECTORY || 'redo_yadamu_' || V_GROUP || '.log'') SIZE 2G REUSE';
    end loop;
  end if;
  for g in (select GROUP# from V$LOG where BYTES < C_BYTES order by GROUP#) loop
    for attempt in 1 .. 10 loop
      select STATUS into V_STATUS from V$LOG where GROUP# = g.GROUP#;
      exit when V_STATUS in ('INACTIVE', 'UNUSED');
      if (V_STATUS = 'CURRENT') then
        execute immediate 'ALTER SYSTEM SWITCH LOGFILE';
      end if;
      execute immediate 'ALTER SYSTEM CHECKPOINT';
    end loop;
    execute immediate 'ALTER DATABASE DROP LOGFILE GROUP ' || g.GROUP#;
  end loop;
end;
/
select GROUP#, THREAD#, BYTES/1024/1024 MB, MEMBERS, STATUS from V$LOG order by GROUP#
/
