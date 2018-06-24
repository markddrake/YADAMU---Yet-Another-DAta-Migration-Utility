---
layout: post
title:  "Part II: Package JSON_EXPORT"
date:   2018-06-15 18:00:00-0700
categories: JSON Export Import Oracle
---

## Automatically generating JSON from an Oracle schema

The [previous]({% link _posts/2018-06-14-Introduction.md%}) post in this blog showed how to generate a JSON representation of the contents of an Oracle database schema. This post outlines the basics of a PL/SQL package that can automate the process of exporting an entire database schema, or a subset of a database schema as a JSON document. 

The previous post mentioned that in (an unpatched) 12cR2 database, JSON operators cannot  generate documents larger that  32K, and that in Oracle 18 this limit no longer applies. In an attempt to avoid the 32K limit on JSON generation in  Oracle 12.2 , the PL/SQL package will use PL/SQL conditional compilation to bypass the use of JSON_QUERY and JSON_ARRAYAGG in databases where CLOB is not a supported return type.

Conditional compilation requires a PL/SQL package is required that provides static constants that accurately describe the state of the database. In this project the package JSON_FEATURE_DETECTION exposes constants TREAT_AS_JSON_SUPPORTED, CLOB_SUPPORED and EXTENDED_STRING_SUPPORTED. TREAT_AS_JSON_SUPPORTED is set TRUE if the database supports the TREAT( AS JSON) feature. Treat as JSON is supported starting with Oracle 18. CLOB_SUPPORTED will be set TRUE in all 18c databases as well as patched 12.2 databases.  EXTENDED_STRING_SUPPORTED will be set true in database where extended data type support has been enabled  

The JSON_FEATURE_DETECTION  package is generated dynamically by a PL/SQL block that uses 'Duck Typing' to detect which features are supported. Duck Typing, for this not familiar with the term, is based on the assumption that if it "walks like a duck", "swims like a duck" and "quacks like a duck" that is is "probably a member of Anatidae family", eg a DUCK. In this case the lack of each feature is detected by executing a SQL statement that makes use of the feature and catching the exception thrown if the feature in question is not supported by the database. 

At first glance creating a PL/SQL package that will generate the required SQL seems quite simple. All that is needed is a list of the tables, along with the set of columns for each table. This appears to be a perfect fit for the LISTAGG operator. 

```SQL
select TABLE_NAME, LISTAGG(COLUMN_NAME,',') WITHIN GROUP (ORDER BY COLUMN_ID) COLUMN_LIST
  from ALL_TAB_COLUMNS
 where OWNER = 'HR'
 group by TABLE_NAME
```



| TABLE_NAME       | COLUMN_LIST                                                  |
| ---------------- | ------------------------------------------------------------ |
| COUNTRIES        | COUNTRY_ID,COUNTRY_NAME,REGION_ID                            |
| DEPARTMENTS      | DEPARTMENT_ID,DEPARTMENT_NAME,MANAGER_ID,LOCATION_ID         |
| EMPLOYEES        | EMPLOYEE_ID,FIRST_NAME,LAST_NAME,EMAIL,PHONE_NUMBER,HIRE_DATE,JOB_ID,SALARY,COMMISSION_PCT,MANAGER_ID,DEPARTMENT_ID |
| EMP_DETAILS_VIEW | EMPLOYEE_ID,JOB_ID,MANAGER_ID,DEPARTMENT_ID,LOCATION_ID,COUNTRY_ID,FIRST_NAME,LAST_NAME,SALARY,COMMISSION_PCT,DEPARTMENT_NAME,JOB_TITLE,CITY,STATE_PROVINCE,COUNTRY_NAME,REGION_NAME |
| JOBS             | JOB_ID,JOB_TITLE,MIN_SALARY,MAX_SALARY                       |
| JOB_HISTORY      | EMPLOYEE_ID,START_DATE,END_DATE,JOB_ID,DEPARTMENT_ID         |
| LOCATIONS        | LOCATION_ID,STREET_ADDRESS,POSTAL_CODE,CITY,STATE_PROVINCE,COUNTRY_ID |
| REGIONS          | REGION_ID,REGION_NAME                                        |

Unfortunately if we do the math, in a a database that supports 128 character identifiers, the results of this query could easily exceed the maximum size supported by LISTAGG. Consequently the column list is captured as a collection, using a CAST(COLLECT *COLUMN_NAME*) AS *TABLE_TYPE_OBECT*) operation. The table is then converted into a list using a PL/SQL function that returns a CLOB. The type T_VC4000_TABLE is defined for this purpose.

The JSON_EXPORT package is split into two main areas of functionality. The first, encapsulated by the function GENERATE_STATEMENT, generates the SQL statement that will be used to export the contents of the schema as JSON. The second, encapsulated in the function EXECUTE_STAETMENT executes the generated statement and returns the resulting JSON document.

The initial version of the GENERATE_STATEMENT procedure is shown below

```SQL
procedure GENERATE_STATEMENT(P_SCHEMA VARCHAR2)
as
  V_SQL_FRAGMENT  VARCHAR2(32767);

  $IF JSON_FEATURE_DETECTION.CLOB_SUPPORTED $THEN
	V_RETURN_TYPE VARCHAR2(32) := 'CLOB';
  $ELSIF JSON_FEATURE_DETECTION.EXTENDED_STRING_SUPPORTED $THEN
	V_RETURN_TYPE VARCHAR2(32):= 'VARCHAR2(32767)';
  $ELSE
    V_RETURN_TYPE VARCHAR2(32):= 'VARCHAR2(4000)';
  $END  

  cursor getTableMetadata
  is
  select aat.owner
        ,aat.table_name
		,cast(collect('"' || COLUMN_NAME || '"' ORDER BY COLUMN_ID) 
              as T_VC4000_TABLE) COLUMN_LIST
    from ALL_ALL_TABLES aat
	     inner join ALL_TAB_COLUMNS atc
		         on atc.OWNER = aat.OWNER
		        and atc.TABLE_NAME = aat.TABLE_NAME
   where aat.OWNER = P_SCHEMA
   group by aat.OWNER, aat.TABLE_NAME;
    
  V_FIRST_ROW BOOLEAN := TRUE;
begin

  DBMS_LOB.CREATETEMPORARY(SQL_STATEMENT,TRUE,DBMS_LOB.CALL);
  V_SQL_FRAGMENT := 'select JSON_OBJECT(''data'' value JSON_OBJECT (' || C_NEWLINE;
  DBMS_LOB.WRITEAPPEND(SQL_STATEMENT,LENGTH(V_SQL_FRAGMENT),V_SQL_FRAGMENT);

  for t in getTableMetadata loop  
	V_SQL_FRAGMENT := C_SINGLE_QUOTE || t.TABLE_NAME || C_SINGLE_QUOTE 
	               || ' value ( select JSON_ARRAYAGG(JSON_ARRAY(';
    if (NOT V_FIRST_ROW) then
      V_SQL_FRAGMENT := ',' || V_SQL_FRAGMENT;
	end if;
	V_FIRST_ROW := FALSE;
	DBMS_LOB.WRITEAPPEND(SQL_STATEMENT,LENGTH(V_SQL_FRAGMENT),V_SQL_FRAGMENT);
	DBMS_LOB.APPEND(SQL_STATEMENT,TABLE_TO_LIST(t.COLUMN_LIST));
    V_SQL_FRAGMENT := ' NULL ON NULL returning ' || V_RETURN_TYPE || ') returning '
                   || V_RETURN_TYPE || ') FROM "' 
                   || t.OWNER || '"."' || t.TABLE_NAME || '")' || C_NEWLINE;
	DBMS_LOB.WRITEAPPEND(SQL_STATEMENT,LENGTH(V_SQL_FRAGMENT),V_SQL_FRAGMENT);
  end loop;

  V_SQL_FRAGMENT := '             returning ' || V_RETURN_TYPE || C_NEWLINE
                 || '           )' || C_NEWLINE
                 || '         returning ' || V_RETURN_TYPE || C_NEWLINE
                 || '       )' || C_NEWLINE
                 || '  from DUAL';
  DBMS_LOB.WRITEAPPEND(SQL_STATEMENT,LENGTH(V_SQL_FRAGMENT),V_SQL_FRAGMENT);
end;
```
It basically queries the data dictionary tables ALL_TAB_COLUMNS and ALL_ALL_TABLES to generate the list of columns for each table in the schema and then iterates over the result set constructing a JSON_ARRAYAGG sub-query for each table. Note how it uses conditional compilation to determine what return type will be used with the SQL operators. The generated SQL is written into the CLOB variable SQL_STATEMENT.

The initial version of the EXPORT_SCHEMA function is shown below

```SQL
function EXPORT_SCHEMA(P_SCHEMA VARCHAR2)
return CLOB
as
  V_JSON_DOCUMENT CLOB;
  V_CURSOR        SYS_REFCURSOR;
begin
  GENERATE_STATEMENT(P_SCHEMA);
  OPEN V_CURSOR FOR SQL_STATEMENT;
  FETCH V_CURSOR INTO V_JSON_DOCUMENT;
  CLOSE V_CURSOR;
  return V_JSON_DOCUMENT;
end;
```

This function takes the output of GENERATE_STATEMENT, and executes it using a cursor. Since the generated statement only returns a single row the function  simply opens the cursor, fetches the row  and closes the cursor. 

The JSON export file can now be generated by simply calling the EXPORT_SCHEMA function. 

```SQL
select JSON_EXPORT.EXPORT_SCHEMA('HR') from dual;
```

```JSON
{
   "data" : {
      "JOBS" : [["AD_PRES","President",20080,40000]
               ,["AD_VP","Administration Vice President",15000,30000]
               ,....
               ,["PR_REP","Public Relations Representative",4500,10500]
               ],
      "REGIONS" : [[1,"Europe"],[2,"Americas"],[3,"Asia"],[4,"Middle East and Africa"]],
      "COUNTRIES" : [[...
```

After invoking the EXPORT_SCHEMA method, the method DUMP_SQL_STATEMENT can be used to retrieve the SQL generated by the GENERTE_STATEMENT method.

The next challenge is what happens with a larger dataset, such as the 'SH' schema. Invoking EXPORT_SCHEMA for the 'SH' schema results in the following error

```SQL
select JSON_EXPORT.EXPORT_SCHEMA('SH') from dual;
```

```
ORA-40654: Input to JSON generation function has unsupported data type.
```

Looking at the data types used by the tables in the SH schema, it appears that there are two tables that contain ROWID columns. ROWID is one a number of data types not currently supported by the JSON generation functions, Fortunately in this case these tables are not  schema data tables, but are secondary tables, used by the Oracle Text index and consequently their content should not be included in the export file. 

To prevent unwanted tables from being included in the output file, the SQL used by GENERATE_TABLES is modified to exclude the following types of table: INVALID TABLES, DROPPED TABLES, TEMORARY_TABLES, EXTERNAL_TABLES, SECONDARY_TABLES, NESTED_TABLES and IOT_OVERFLOW tables. OBJECT tables are also excluded for the moment, as they will require special handling. After applying the filter conditions the select statement used by GENERATE_TABLES is as follows 

```SQL
 select aat.owner
        ,aat.table_name
		,cast(collect('"' || COLUMN_NAME || '"' ORDER BY COLUMN_ID) as T_VC4000_TABLE) 
		 COLUMN_LIST
    from ALL_ALL_TABLES aat
	     inner join ALL_TAB_COLUMNS atc
		         on atc.OWNER = aat.OWNER
		        and atc.TABLE_NAME = aat.TABLE_NAME
   where aat.STATUS = 'VALID'
     and aat.DROPPED = 'NO'
	 and aat.TEMPORARY = 'N'
     and aat.EXTERNAL = 'NO'
	 and aat.NESTED = 'NO'
	 and aat.SECONDARY = 'N'
	 and (aat.IOT_TYPE is NULL or aat.IOT_TYPE = 'IOT')
	 and aat.TABLE_TYPE is NULL
	 and aat.OWNER = P_SCHEMA
   group by aat.OWNER, aat.TABLE_NAME;
```

After modifying the GENERATE_STATEMENT method, invoking EXPORT_SCHEMA on the SH schema in an unpatched 12.2 results in

```
ORA-40459: output value too large (actual: 32801, maximum: 32767)
```

Since generating a JSON representation of the SH schema results in a document much larger than 32K. One way of avoiding this problem is to limit the number of rows processed for each table in the schema. This can done by making it possible set a row limit and then using row limit value in a WHERE clause that restricts the number of rows passed to the JSON_ARRAYAGG operations. Limiting the number of rows processed by the EXPORT_SCHEMA function somewhat defeats the purpose of an export utility. Clearly an alternative solution is required in databases where CLOB is not a supported return type for the JSON operators. 

One way of approaching this is to manually print the output the JSON_ARRAY operations directly into a CLOB. In general, using string concatenation to generate JSON is a really bad idea. In particular there are a lot of nuances around correctly handling special characters in text values, and it is very easy to make mistakes that result in the creation of invalid JSON documents.  However in this case, string concatenation is an acceptable approach, since the all string conversions are handled by the JSON_ARRAY operator and the results can safely be combined to form the desired output.  

The 12.2 implementation of GENERATE_STATEMENT method returns a SQL statement for each table, rather than one statement for the entire schema. This allow the results for each table to inserted as a distinct key in the main document.  

```SQL
declare
    V_SQL_STATEMENT CLOB;
begin
  EXPORT_METADATA_CACHE := T_EXPORT_METADATA_TABLE();
  for t in getTableMetadata loop  
    EXPORT_METADATA_CACHE.extend();
    EXPORT_METADATA_CACHE(EXPORT_METADATA_CACHE.count).OWNER := t.OWNER;
    EXPORT_METADATA_CACHE(EXPORT_METADATA_CACHE.count).TABLE_NAME := t.TABLE_NAME;
    DBMS_LOB.CREATETEMPORARY(V_SQL_STATEMENT,TRUE,DBMS_LOB.CALL);
    V_SQL_FRAGMENT := 'select JSON_ARRAY(';
    DBMS_LOB.WRITEAPPEND(V_SQL_STATEMENT,LENGTH(V_SQL_FRAGMENT),V_SQL_FRAGMENT);
    DBMS_LOB.APPEND(V_SQL_STATEMENT,TABLE_TO_LIST(t.COLUMN_LIST));
    V_SQL_FRAGMENT := ' NULL on NULL returning '|| V_RETURN_TYPE || ') FROM "' 
                   || t.OWNER || '"."' || t.TABLE_NAME || '" ';
    if (ROW_LIMIT > -1) then
	  V_SQL_FRAGMENT := V_SQL_FRAGMENT || 'WHERE ROWNUM < ' || ROW_LIMIT;
	end if;
    DBMS_LOB.WRITEAPPEND(V_SQL_STATEMENT,LENGTH(V_SQL_FRAGMENT),V_SQL_FRAGMENT);
    EXPORT_METADATA_CACHE(EXPORT_METADATA_CACHE.count).SQL_STATEMENT := V_SQL_STATEMENT;	
  end loop;
end;
```
The 12.2 implementation of EXPORT_SCHEMA starts by printing the outer  *{ "data" :*  to the CLOB. Next it loops over the set of SQL statements produced by GENERATE_STATEMENT. For each statement it  appends a key based on the table name whose value is an array. It then executes the SQL for that table and appends the rows returned by the query as the members of the array. Conditional compilation, based on CLOB_SUPPORTED, is used in both implementations to determine which path is executed.

```SQL
function EXPORT_SCHEMA(P_SCHEMA VARCHAR2)
return CLOB
as
  V_JSON_DOCUMENT CLOB;
  V_CURSOR        SYS_REFCURSOR;

  V_JSON_FRAGMENT VARCHAR2(4000);

  V_FIRST_TABLE   BOOLEAN := TRUE;
  V_FIRST_ITEM    BOOLEAN := TRUE;

  $IF JSON_FEATURE_DETECTION.EXTENDED_STRING_SUPPORTED $THEN
  V_JSON_ARRAY VARCHAR2(32767);
  $ELSE
  V_JSON_ARRAY VARCHAR2(4000);
  $END  

begin
  DBMS_LOB.CREATETEMPORARY(V_JSON_DOCUMENT,TRUE,DBMS_LOB.CALL);
  V_JSON_FRAGMENT := '{"data":{';
  DBMS_LOB.WRITEAPPEND(V_JSON_DOCUMENT,length(V_JSON_FRAGMENT),V_JSON_FRAGMENT);
  GENERATE_STATEMENT(P_SCHEMA);
  for i in 1 .. EXPORT_METADATA_CACHE.count loop
    V_JSON_FRAGMENT := '"' || EXPORT_METADATA_CACHE(i).table_name || '":[';
	if (not V_FIRST_TABLE) then 
  	  V_JSON_FRAGMENT := ',' || V_JSON_FRAGMENT;
	end if;
	V_FIRST_TABLE := false;
	DBMS_LOB.WRITEAPPEND(V_JSON_DOCUMENT,length(V_JSON_FRAGMENT),V_JSON_FRAGMENT);
    V_FIRST_ITEM := TRUE;
    OPEN V_CURSOR for EXPORT_METADATA_CACHE(i).SQL_STATEMENT;
	loop
	  FETCH V_CURSOR into V_JSON_ARRAY;
	  EXIT WHEN V_CURSOR%notfound;	  
	  if (NOT V_FIRST_ITEM) then
    	DBMS_LOB.WRITEAPPEND(V_JSON_DOCUMENT,1,',');
	  end if;
 	  V_FIRST_ITEM := FALSE;
      DBMS_LOB.WRITEAPPEND(V_JSON_DOCUMENT,LENGTH(V_JSON_ARRAY),V_JSON_ARRAY);
	end loop;
	CLOSE V_CURSOR;
    DBMS_LOB.WRITEAPPEND(V_JSON_DOCUMENT,1,']');
  end loop;
  DBMS_LOB.WRITEAPPEND(V_JSON_DOCUMENT,1,'}');
  DBMS_LOB.WRITEAPPEND(V_JSON_DOCUMENT,1,'}');
  return V_JSON_DOCUMENT;
end;
```
The EXPORT_SCHEMA function can now successfully return the full content of the both the HR and SH schemas, even in a 12.2 database where the JSON operators do support CLOB.

```SQL
SQL> select JSON_EXPORT.EXPORT_SCHEMA('HR') from dual
```

```JSON
{"data":{"JOBS":[["AD_PRES","President",20080,40000],["AD_VP","Administration Vic
```



```SQL
SQL> select JSON_EXPORT.EXPORT_SCHEMA('SH') from dual;
```

```JSON
{"data":{"COSTS":[],"SALES":[[13,987,"1998-01-10T00:00:00",3,999,1,1232.16],[13,1
```



```SQl
SQL> select DBMS_LOB.GETLENGTH(JSON_EXPORT.EXPORT_SCHEMA('SH')) from dual;
```

```SQL
58582866
```

 As can be seen the size of the document generated when processing the SH schema is almost 60MB.  The IS JSON condition can be used to verify that the generated document is valid JSON.

```SQL
SQL> select 1 "VALID JSON" from dual where JSON_EXPORT.EXPORT_SCHEMA('SH') is JSON;
```

```SQL
VALID JSON
----------
         1
1 row selected.
```
Note this statement would return no rows if the generated document was not valid JSON.

A quick and the dirty was to output the generated document to a file is to use a simple SQL*PLUS script which spools the results to a file.

```SQL
DEF SCHEMA_NAME = &1
set lines 1024
column JSON format A1024
set feedback off
set heading off
set termout off
set verify off
set long 1000000000
set pages 0
set echo off
spool JSON/&SCHEMA_NAME..json
select JSON_EXPORT.EXPORT_SCHEMA('&SCHEMA_NAME') JSON from dual;
spool off
set echo on
set pages 100
set verify on
set termout on
set heading on
set feedback on
```

Unfortunately SQL*PLUS inserts 'hard' end-of-line characters into the JSON after each block of 1024 characters, these need to be stripped out before the file can be processed as JSON.

The [next]({% link _posts/2018-06-16-Unsupported Scalar Types.md%}) post will investigate what happens when we attempt to export the OE schema, which contains a much richer set of Oracle data types, than the HR and SH schemas.