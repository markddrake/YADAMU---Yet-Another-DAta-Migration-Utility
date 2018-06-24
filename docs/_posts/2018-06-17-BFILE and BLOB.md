---
layout: post
title:  "Part IV: Unsupported data types (2)"
date:   2018-06-17 18:00:00-0700
categories: JSON Export Import Oracle
---

## Supporting BFILE and BLOB using in-line PL/SQL

The [previous]({% link _posts/2018-06-16-Unsupported Scalar Types.md%}) post showed how to use SQL conversions to extend the set of data types that can be used with JSON_ARRAY. This post will build on that concept showing how in-line PL/SQL can be used in places where pure SQL is not sufficient. 

JSON_ARRAY does not support the BFILE data type in any current release of the Oracle Database. A BFILE is basically a pointer to a file stored outside the database. The pointer consists of two components, a SQL Directory name and a path to the file, relative to the folder associated with the directory. There are 2 approaches to supporting the BFILE datatype.  The first is to inline the content of the file directly into the export, the second is to serialize the BFILE as a directory name and a relative path. In the current implementation of EXPORT_SCHEMA the second option is used.

It is not possible to extract this information directly a from BFILE column using SQL, so PL/SQL needs to be used.  The PL/SQL is extremely simple, given a BFILE column the method DBMS_LOB.FILEGETNAME will return the directory name and filename. However, since the FILEGETNAME method uses OUT parameters to return the required information it cannot be called directly from SQL, so a PL/SQL wrapper is required to allow FILEGETNAME to be invoked as part of the JSON_ARRAY operation. 

The wrapper function is show below

```SQL
function BFILE2CHAR(P_BFILE BFILE) return VARCHAR2
as
  V_SINGLE_QUOTE     CONSTANT CHAR(1) := CHR(39);
  V_DIRECTORY_ALIAS  VARCHAR2(128 CHAR);
  V_PATH2FILE        VARCHAR2(2000 CHAR);
begin
  DBMS_LOB.FILEGETNAME(P_BFILE,V_DIRECTORY_ALIAS,V_PATH2FILE);
  return 'BFILENAME(' || V_SINGLE_QUOTE || V_DIRECTORY_ALIAS || V_SINGLE_QUOTE || ',' 
                      || V_SINGLE_QUOTE || V_PATH2FILE || V_SINGLE_QUOTE || ')';
end;
```

It returns the directory and file names as the SQL function that was used when the BFILE was created. 

A similar approach is taken in Oracle 12 when dealing with BLOB datatypes. Oracle 18 correctly outputs BLOB content as a HEXBINARY string. This behavior is mimicked in Oracle 12 by supplying  a PL/SQL wrapper that returns a CLOB containing the HEXBINARY representation of the BLOB.  The maximum size that can be handled using this approach is limited based on the return type specified for the JSON_ARRAY operation. Consequently, an error will be returned if the HEXBINARY representation of the input document is larger than the maximum size supported by the JSON_ARRAY operator. The size check assumes that the BLOB column is the only input to JSON_ARRAY, and it's primary use is to avoid the overhead of serializing large documents in cases where the generated output cannot be processed further.

The following changes were made to the case statement in GENERATE_SCHEMA. For handling BLOBS in release 12

```SQL
/*
** 18.1 compatible handling of BLOB
*/
when DATA_TYPE = 'BLOB'
  then 'BLOB2HEXBINARY("' || COLUMN_NAME || '")' 	
```
and for handling BFILES in all releases

```SQL
/*
** Fix for BFILENAME
*/
when DATA_TYPE = 'BFILE'
  then 'BFILE2CHAR("' || COLUMN_NAME || '")'
```
In order to enable the use of these functions from EXPORT_SCHEMA a WITH clause containing the PL/SQL code is added the SQL statement(s) created by GENERATE_SCHEMA. To ensure that the WITH block is only added when necessary, the query in GENERATE_SCHEMA is modified to count the number of BLOB and BFILE columns for each table as shown below

```SQL
select aat.owner
        ,aat.table_name
  	    ,SUM(CASE WHEN DATA_TYPE = 'BLOB'  THEN 1 ELSE 0 END) BLOB_COUNT
  	    ,SUM(CASE WHEN DATA_TYPE = 'BFILE' THEN 1 ELSE 0 END) BFILE_COUNT
	    ,cast(collect...
```

A new method, GENERATE_WITH_CLAUSE is added to generate the WITH clause

```SQL
procedure GENERATE_WITH_CLAUSE(P_BFILE_COUNT NUMBER, P_BLOB_COUNT NUMBER, 
                               P_SQL_STATEMENT IN OUT CLOB)
as
begin
  if ((P_BFILE_COUNT = 0) and (P_BLOB_COUNT = 0)) then
    return;
  end if;
  DBMS_LOB.APPEND(P_SQL_STATEMENT,TO_CLOB('WITH' || C_NEWLINE));
  if (P_BFILE_COUNT > 0) then
    DBMS_LOB.APPEND(P_SQL_STATEMENT,TO_CLOB(CODE_BFILE2CHAR));
  end if;
  if (P_BLOB_COUNT > 0) then
    DBMS_LOB.APPEND(P_SQL_STATEMENT,TO_CLOB(CODE_BLOB2HEXBINARY));
  end if;
end;

```
This method is invoked in GENERATE schema prior to printing the 'select' into the LOB that contains the generated SQL statement. The [next]({% link _posts/2018-06-18-Objects and ANYDATA.md%}) post will discuss how a similar technique can be used to support Oracle OBJECT types and the ANYDATA type.