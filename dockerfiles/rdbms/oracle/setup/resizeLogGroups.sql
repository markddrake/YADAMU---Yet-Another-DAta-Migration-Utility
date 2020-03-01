colum GROUP new_value FIRST_GROUP
select group# "GROUP"
  from V$LOG
 where STATUS <> 'CURRENT'
   and ROWNUM = 1
/
COLUMN MEMBER new_value FILE_NAME
select MEMBER 
  from V$LOGFILE
 where GROUP# = &FIRST_GROUP
   and ROWNUM = 1
/
ALTER DATABASE DROP LOGFILE GROUP &FIRST_GROUP
/
ALTER DATABASE 
  ADD LOGFILE GROUP &FIRST_GROUP ('&FILE_NAME') SIZE 250M REUSE
/
select * from V$LOG
/
ALTER SYSTEM SWITCH LOGFILE
/
shutdown immediate
connect / as sysdba
startup
--
select * from V$LOG
/
colum GROUP new_value SECOND_GROUP
select group# "GROUP"
  from V$LOG
 where STATUS <> 'CURRENT'
   and ROWNUM = 1
   and GROUP# <> &FIRST_GROUP
/
COLUMN MEMBER new_value FILE_NAME
select MEMBER 
  from V$LOGFILE
 where GROUP# = &SECOND_GROUP
   and ROWNUM = 1
/
ALTER DATABASE DROP LOGFILE GROUP &SECOND_GROUP
/
ALTER DATABASE 
  ADD LOGFILE GROUP &SECOND_GROUP ('&FILE_NAME') SIZE 250M REUSE
/
select * from V$LOG
/
ALTER SYSTEM SWITCH LOGFILE
/
shutdown immediate
connect / as sysdba
startup
--
select * from V$LOG
/
colum GROUP new_value THIRD_GROUP
select group# "GROUP"
  from V$LOG
 where STATUS <> 'CURRENT'
   and ROWNUM = 1
   and GROUP# <> &FIRST_GROUP
   and GROUP# <> &SECOND_GROUP
/
COLUMN MEMBER new_value FILE_NAME
select MEMBER 
  from V$LOGFILE
 where GROUP# = &THIRD_GROUP
   and ROWNUM = 1
/
ALTER DATABASE DROP LOGFILE GROUP &THIRD_GROUP
/
ALTER DATABASE 
  ADD LOGFILE GROUP &THIRD_GROUP ('&FILE_NAME') SIZE 250M REUSE
/
