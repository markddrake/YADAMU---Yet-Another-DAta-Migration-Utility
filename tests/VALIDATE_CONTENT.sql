--
DEF SOURCE_SCHEMA = &1
--
DEF TARGET_SCHEMA = &2
--
column SOURCE_SCHEMA format A32
column TARGET_SCHEMA format A32
column TABLE_NAME format A32
--
set lines 256
set serveroutput on
--
create global temporary table JSON_CLONE_RESULTS (
  SOURCE_SCHEMA    VARCHAR2(128)
 ,TARGET_SCHEMA    VARCHAR2(128)
 ,TABLE_NAME       VARCHAR2(128)
 ,SOURCE_ROW_COUNT NUMBER
 ,TARGET_ROW_COUNT NUMBER
 ,MISSINGS_ROWS    NUMBER
 ,EXTRA_ROWS       NUMBER
) 
/
declare 
  cursor getTableList
  is
  select aat.TABLE_NAME
        ,LISTAGG(
		   case 
		     when DATA_TYPE = 'BFILE'
			   then 'case when "' || COLUMN_NAME || '" is NULL then NULL else BFILE2CHAR("' || COLUMN_NAME || '") end' 
		     when DATA_TYPE = 'XMLTYPE' 
		       then 'case when "' || COLUMN_NAME || '" is NULL then NULL else dbms_crypto.HASH(XMLSERIALIZE(CONTENT "' || COLUMN_NAME || '" as CLOB),getCyptoHash) end' 
			 when DATA_TYPE in ('NCLOB')
		       then 'case when "' || COLUMN_NAME || '" is NULL then NULL else dbms_crypto.HASH(TO_CLOB("' || COLUMN_NAME || '"),getCyptoHash) end'
			 when DATA_TYPE in ('CLOB','BLOB')
		       then 'case when "' || COLUMN_NAME || '" is NULL then NULL else dbms_crypto.HASH("' || COLUMN_NAME || '",getCyptoHash) end'
			 else
			   '"' || COLUMN_NAME || '"'
		     end,
		',') 
		 WITHIN GROUP (ORDER BY INTERNAL_COLUMN_ID, COLUMN_NAME) COLUMN_LIST
  from ALL_ALL_TABLES aat
       inner join ALL_TAB_COLS atc
	         on atc.OWNER = aat.OWNER
	        and atc.TABLE_NAME = aat.TABLE_NAME
       left outer join ALL_TYPES at
	                on at.TYPE_NAME = atc.DATA_TYPE
                   and at.OWNER = atc.DATA_TYPE_OWNER
	   left outer join ALL_MVIEWS amv
		            on amv.OWNER = aat.OWNER
		           and amv.MVIEW_NAME = aat.TABLE_NAME    
 where aat.STATUS = 'VALID'
   and aat.DROPPED = 'NO'
   and aat.TEMPORARY = 'N'
   and aat.EXTERNAL = 'NO'
   and aat.NESTED = 'NO'
   and aat.SECONDARY = 'N'
   and (aat.IOT_TYPE is NULL or aat.IOT_TYPE = 'IOT')
   and (
	    ((aat.TABLE_TYPE is NULL) and ((atc.HIDDEN_COLUMN = 'NO') and ((atc.VIRTUAL_COLUMN = 'NO') or ((atc.VIRTUAL_COLUMN = 'YES') and (atc.DATA_TYPE = 'XMLTYPE')))))
        or
	    ((aat.TABLE_TYPE is not NULL) and (COLUMN_NAME in ('SYS_NC_OID$','SYS_NC_ROWINFO$')))
	    or
		((aat.TABLE_TYPE = 'XMLTYPE') and (COLUMN_NAME in ('ACLOID', 'OWNERID')))
       )
	and aat.OWNER = '&SOURCE_SCHEMA'
    and ((TYPECODE is NULL) or (TYPE_NAME = 'XMLTYPE'))
  group by aat.TABLE_NAME;
  
  C_NEWLINE         CONSTANT CHAR(1) := CHR(10);
  V_SQL_STATEMENT VARCHAR2(32767);
begin
  for t in getTableList loop
    V_SQL_STATEMENT := 'insert /*+ WITH_PLSQL */ into JSON_CLONE_RESULTS ' || C_NEWLINE
                    || 'with'|| C_NEWLINE
					|| 'function getCyptoHash return number as begin return DBMS_CRYPTO.hash_sh256; end;'|| C_NEWLINE
					|| OBJECT_SERIALIZATION.CODE_BFILE2CHAR 
                    || ' select ''&SOURCE_SCHEMA'' ' || C_NEWLINE
                    || '       ,''&TARGET_SCHEMA'' ' || C_NEWLINE
                    || '       ,'''  || t.TABLE_NAME || ''' ' || C_NEWLINE
                    || '       ,(select count(*) from "&SOURCE_SCHEMA"."' || t.TABLE_NAME || '")'  || C_NEWLINE
                    || '       ,(select count(*) from "&TARGET_SCHEMA"."' || t.TABLE_NAME || '")'  || C_NEWLINE
                    || '       ,(select count(*) from (SELECT ' || t.COLUMN_LIST || ' from "&SOURCE_SCHEMA"."' || t.TABLE_NAME || '" MINUS SELECT ' || t.COLUMN_LIST || ' from  "&TARGET_SCHEMA"."' || t.TABLE_NAME || '")) '  || C_NEWLINE
                    || '       ,(select count(*) from (SELECT ' || t.COLUMN_LIST || ' from "&TARGET_SCHEMA"."' || t.TABLE_NAME || '" MINUS SELECT ' || t.COLUMN_LIST || ' from  "&SOURCE_SCHEMA"."' || t.TABLE_NAME || '")) '  || C_NEWLINE
					|| '  from dual';
	DBMS_OUTPUT.PUT_LINE(V_SQL_STATEMENT);
	EXECUTE IMMEDIATE V_SQL_STATEMENT;
  end loop;
exception
  when OTHERS then 
    DBMS_OUTPUT.PUT_LINE(V_SQL_STATEMENT);					  
	RAISE;
end;
/
show errors
--
select * from JSON_CLONE_RESULTS
/
drop table JSON_CLONE_RESULTS
/