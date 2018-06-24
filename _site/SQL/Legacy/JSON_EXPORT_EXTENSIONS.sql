		   $IF JSON_FEATURE_DETECTION.TREAT_AS_JSON_SUPPORTED
	       case
			 when DATA_TYPE_OWNER is not NULL 
               then case 
                      when TYPECODE = 'COLLECTION'
			            then 'case when "' || COLUMN_NAME || '" is NULL then to_CLOB(''NULL'') else OBJECT_SERIALIZATION.serializeObject(ANYDATA.convertCollection("' || COLUMN_NAME || '")) end'
					  when TYPECODE = 'OBJECT'
  				        then 'case when "' || COLUMN_NAME || '" is NULL then to_CLOB(''NULL'') else OBJECT_SERIALIZATION.serializeObject(ANYDATA.convertObject("' || COLUMN_NAME || '")) end'
					  else 
					    'case when "' || COLUMN_NAME || '" is NULL then to_CLOB(''NULL'') else OBJECT_SERIALIZATION.unknownObjectType(''' || DATA_TYPE_OWNER || ''',''' || DATA_TYPE || ''') end'
					end
			   else case
			         

					  
					           listagg('"' || COLUMN_NAME || '"',',')  WITHIN GROUP (ORDER BY COLUMN_ID, COLUMN_NAME) COLUMN_LIST
        ,listagg(
	       '"' || DATA_TYPE ||
	       case
             when DATA_TYPE = 'NUMBER' 
               then case 
                      when DATA_SCALE is NOT NULL and DATA_SCALE <> 0
                        then '(' || DATA_PRECISION || ',' || DATA_SCALE || ')'
                      when DATA_PRECISION is NOT NULL
                        then '(' || DATA_PRECISION || ')'
                      else
                        null -- '(38)'
                    end 
             when DATA_TYPE = 'FLOAT' 
               then '(' || DATA_PRECISION || ')'
   	         when DATA_TYPE in ('VARCHAR2', 'CHAR', 'NVARCHAR2') and (CHAR_LENGTH < DATA_LENGTH) 
               then '(' || CHAR_LENGTH || ' CHAR)'
	         when DATA_TYPE in ('VARCHAR2', 'CHAR', 'NVARCHAR2', 'UROWID')  
               then '(' || DATA_LENGTH || ')'
             when DATA_TYPE = 'RAW' 
               then '(' || (DATA_LENGTH * 2) ||')'
             when DATA_TYPE LIKE 'INTERVAL%' 
               then '(32)'
	       end
		   || '"',','
	     ) WITHIN GROUP (ORDER BY COLUMN_ID, COLUMN_NAME) DATA_TYPE_LIST
        ,listagg(
           '"' || COLUMN_NAME || '"'
           ,','
	     ) WITHIN GROUP (ORDER BY COLUMN_ID, COLUMN_NAME) SQL_STATEMENT
		,listagg('"' || COLUMN_NAME || '"',',')  WITHIN GROUP (ORDER BY COLUMN_ID, COLUMN_NAME) IMPORT_SELECT_LIST
		,NULL JSON_TABLE_COLUMNS_CLAUSE