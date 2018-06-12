--
DEF SOURCE_SCHEMA = &1
--
DEF TARGET_SCHEMA = &2
--
set lines 256 pages 50
--
column SCHEMA format A32
column SOURCE_SCHEMA format A32
column TARGET_SCHEMA format A32
column OBJECT_NAME format A32
column OBJECT_TYPE format A32
--
with 
SOURCE_SCHEMA_OBJECTS as (
  select DISTINCT OWNER, OBJECT_NAME, OBJECT_TYPE
    from ALL_OBJECTS ao
   where ao.OWNER = '&SOURCE_SCHEMA'
     and not (( ao.OBJECT_TYPE = 'LOB') and (ao.OBJECT_NAME like 'SYS_LOB%$$'))
     and not (( ao.OBJECT_TYPE = 'INDEX') and (ao.OBJECT_NAME like 'SYS_IL%$$'))
	 and not (( ao.OBJECT_TYPE = 'TABLE') and (ao.OBJECT_NAME like 'SYS_IOT_OVER_%'))
	 and not (( ao.OBJECT_TYPE = 'TYPE') and ((ao.OBJECT_NAME like 'SYS_YOID%$') or (ao.OBJECT_NAME like 'ST00001%=') or (ao.OBJECT_NAME like 'SYS_PLSQL_%')))
),
TARGET_SCHEMA_OBJECTS as (
  select DISTINCT OWNER, OBJECT_NAME, OBJECT_TYPE
    from ALL_OBJECTS ao
   where ao.OWNER = '&TARGET_SCHEMA'
     and not (( ao.OBJECT_TYPE = 'LOB') and (ao.OBJECT_NAME like 'SYS_LOB%$$'))
     and not (( ao.OBJECT_TYPE = 'INDEX') and (ao.OBJECT_NAME like 'SYS_IL%$$'))
	 and not (( ao.OBJECT_TYPE = 'TABLE') and (ao.OBJECT_NAME like 'SYS_IOT_OVER_%'))
	 and not (( ao.OBJECT_TYPE = 'TYPE') and (ao.OBJECT_NAME like 'SYS_YOID%$') )
	 and not (( ao.OBJECT_TYPE = 'TYPE') and ((ao.OBJECT_NAME like 'SYS_YOID%$') or (ao.OBJECT_NAME like 'ST00001%=') or (ao.OBJECT_NAME like 'SYS_PLSQL_%')))
)
select (
         select count(*) 
		   from SOURCE_SCHEMA_OBJECTS
	   ) "&SOURCE_SCHEMA"
	  ,(
         select count(*) 
		   from TARGET_SCHEMA_OBJECTS
		) "&TARGET_SCHEMA"
  from dual
/  
with 
SOURCE_SCHEMA_OBJECTS as (
  select DISTINCT OWNER, OBJECT_NAME, OBJECT_TYPE
    from ALL_OBJECTS ao
   where ao.OWNER = '&SOURCE_SCHEMA'
     and not (( ao.OBJECT_TYPE = 'LOB') and (ao.OBJECT_NAME like 'SYS_LOB%$$'))
     and not (( ao.OBJECT_TYPE = 'INDEX') and (ao.OBJECT_NAME like 'SYS_IL%$$'))
	 and not (( ao.OBJECT_TYPE = 'TABLE') and (ao.OBJECT_NAME like 'SYS_IOT_OVER_%'))
	 and not (( ao.OBJECT_TYPE = 'TYPE') and (ao.OBJECT_NAME like 'SYS_YOID%$'))
	 and not (( ao.OBJECT_TYPE = 'TYPE') and ((ao.OBJECT_NAME like 'SYS_YOID%$') or (ao.OBJECT_NAME like 'ST00001%=') or (ao.OBJECT_NAME like 'SYS_PLSQL_%')))
),
TARGET_SCHEMA_OBJECTS as (
  select DISTINCT OWNER, OBJECT_NAME, OBJECT_TYPE
    from ALL_OBJECTS ao
   where ao.OWNER = '&TARGET_SCHEMA'
     and not (( ao.OBJECT_TYPE = 'LOB') and (ao.OBJECT_NAME like 'SYS_LOB%$$'))
     and not (( ao.OBJECT_TYPE = 'INDEX') and (ao.OBJECT_NAME like 'SYS_IL%$$'))
	 and not (( ao.OBJECT_TYPE = 'TABLE') and (ao.OBJECT_NAME like 'SYS_IOT_OVER_%'))
	 and not (( ao.OBJECT_TYPE = 'TYPE') and (ao.OBJECT_NAME like 'SYS_YOID%$'))
	 and not (( ao.OBJECT_TYPE = 'TYPE') and ((ao.OBJECT_NAME like 'SYS_YOID%$') or (ao.OBJECT_NAME like 'ST00001%=') or (ao.OBJECT_NAME like 'SYS_PLSQL_%')))
)
SELECT sso.OWNER SOURCE_SCHEMA, tso.OWNER TARGET_SCHEMA, sso.OBJECT_TYPE , sso.OBJECT_NAME
  from SOURCE_SCHEMA_OBJECTS sso
       INNER JOIN TARGET_SCHEMA_OBJECTS tso
		 on sso.OBJECT_NAME = tso.OBJECT_NAME
		and sso.OBJECT_TYPE = tso.OBJECT_TYPE  
  order by OBJECT_TYPE, OBJECT_NAME
/

with 
SOURCE_SCHEMA_OBJECTS as (
  select DISTINCT OWNER, OBJECT_NAME, OBJECT_TYPE
    from ALL_OBJECTS ao
   where ao.OWNER = '&SOURCE_SCHEMA'
     and not (( ao.OBJECT_TYPE = 'LOB') and (ao.OBJECT_NAME like 'SYS_LOB%$$'))
     and not (( ao.OBJECT_TYPE = 'INDEX') and (ao.OBJECT_NAME like 'SYS_IL%$$'))
	 and not (( ao.OBJECT_TYPE = 'TABLE') and (ao.OBJECT_NAME like 'SYS_IOT_OVER_%'))
	 and not (( ao.OBJECT_TYPE = 'TYPE') and (ao.OBJECT_NAME like 'SYS_YOID%$'))
	 and not (( ao.OBJECT_TYPE = 'TYPE') and ((ao.OBJECT_NAME like 'SYS_YOID%$') or (ao.OBJECT_NAME like 'ST00001%=') or (ao.OBJECT_NAME like 'SYS_PLSQL_%')))
),
TARGET_SCHEMA_OBJECTS as (
  select DISTINCT OWNER, OBJECT_NAME, OBJECT_TYPE
    from ALL_OBJECTS ao
   where ao.OWNER = '&TARGET_SCHEMA'
     and not (( ao.OBJECT_TYPE = 'LOB') and (ao.OBJECT_NAME like 'SYS_LOB%$$'))
     and not (( ao.OBJECT_TYPE = 'INDEX') and (ao.OBJECT_NAME like 'SYS_IL%$$'))
	 and not (( ao.OBJECT_TYPE = 'TABLE') and (ao.OBJECT_NAME like 'SYS_IOT_OVER_%'))
	 and not (( ao.OBJECT_TYPE = 'TYPE') and (ao.OBJECT_NAME like 'SYS_YOID%$'))
	 and not (( ao.OBJECT_TYPE = 'TYPE') and ((ao.OBJECT_NAME like 'SYS_YOID%$') or (ao.OBJECT_NAME like 'ST00001%=') or (ao.OBJECT_NAME like 'SYS_PLSQL_%')))
)
SELECT sso.OWNER SCHEMA, sso.OBJECT_NAME, sso.OBJECT_TYPE, 'Missing in "&TARGET_SCHEMA"' STATUS
  from SOURCE_SCHEMA_OBJECTS sso
       LEFT OUTER JOIN TARGET_SCHEMA_OBJECTS tso
		 on tso.OBJECT_NAME = sso.OBJECT_NAME
		and tso.OBJECT_TYPE = sso.OBJECT_TYPE  
  where tso.OBJECT_NAME is NULL
union all
SELECT tso.OWNER SCHEMA, tso.OBJECT_NAME, tso.OBJECT_TYPE, 'Missing in "&SOURCE_SCHEMA"' STATUS
  from TARGET_SCHEMA_OBJECTS tso
       LEFT OUTER JOIN SOURCE_SCHEMA_OBJECTS sso
		 on sso.OBJECT_NAME = tso.OBJECT_NAME
		and sso.OBJECT_TYPE = tso.OBJECT_TYPE  
  where sso.OBJECT_NAME is NULL
  order by SCHEMA, OBJECT_TYPE, OBJECT_NAME
/