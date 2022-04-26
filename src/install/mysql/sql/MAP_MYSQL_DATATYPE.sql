SET SESSION SQL_MODE=ANSI_QUOTES;
--
DROP FUNCTION IF EXISTS MAP_MYSQL_DATATYPE;
--
DELIMITER $$
--
CREATE FUNCTION MAP_MYSQL_DATATYPE(P_VENDOR VARCHAR(32), P_MYSQL_DATA_TYPE VARCHAR(256), P_DATA_TYPE VARCHAR(256), P_DATA_TYPE_LENGTH INT, P_DATA_TYPE_SCALE INT, P_CIRCLE_FORMAT VARCHAR(7)) 
RETURNS CHAR(128) DETERMINISTIC
BEGIN
  DECLARE C_TINYTEXT_LENGTH            SMALLINT DEFAULT 255;
  DECLARE C_TEXT_LENGTH                     INT DEFAULT 65535;
  DECLARE C_MEDIUMTEXT_LENGTH               INT DEFAULT 16777215;
  DECLARE C_LONGTEXT_LENGTH              BIGINT DEFAULT 4294967295;
									 
  DECLARE C_TINYBLOB_LENGTH            SMALLINT DEFAULT 255;
  DECLARE C_BLOB_LENGTH                     INT DEFAULT 65535;
  DECLARE C_MEDIUMBLOB_LENGTH               INT DEFAULT 16777215;
  DECLARE C_LONGBLOB_LENGTH              BIGINT DEFAULT 4294967295;
									 
  DECLARE C_CHAR_LENGTH                SMALLINT DEFAULT 255;
  DECLARE C_BINARY_LENGTH              SMALLINT DEFAULT 255;
  DECLARE C_VARCHAR_LENGTH             SMALLINT DEFAULT 4096;
  DECLARE C_VARBINARY_LENGTH           SMALLINT DEFAULT 8192;
  
  DECLARE C_NUMERIC_PRECISION          SMALLINT DEFAULT 65;
  DECLARE C_NUMERIC_SCALE              SMALLINT DEFAULT 30;
  
  DECLARE C_CLOB_TYPE               VARCHAR(32) DEFAULT 'longtext'; 

  DECLARE C_LARGEST_CHAR_TYPE       VARCHAR(32) DEFAULT concat('char(',C_CHAR_LENGTH,')');
  DECLARE C_LARGEST_BINARY_TYPE     VARCHAR(32) DEFAULT concat('binary(',C_BINARY_LENGTH,')');
  DECLARE C_LARGEST_VARCHAR_TYPE    VARCHAR(32) DEFAULT concat('varchar(',C_VARCHAR_LENGTH,')');
  DECLARE C_LARGEST_VABRINARY_TYPE  VARCHAR(32) DEFAULT concat('varbinary(',C_VARBINARY_LENGTH,')');
  
  DECLARE C_UNBOUNDED_NUMERIC       VARCHAR(32) DEFAULT 'decimal(65,30)';
  
  DECLARE C_ORACLE_OBJECT_TYPE      VARCHAR(32) DEFAULT C_CLOB_TYPE;
  
  DECLARE ERROR_MSG                 VARCHAR(256) DEFAULT concat('MySQL: ','Missing mapping for "',P_VENDOR,'" datatype "',P_DATA_TYPE,'"');
  
  case 
    when ((P_MYSQL_DATA_TYPE is NULL) and (P_Vendor = 'Oracle') and (P_DATA_TYPE like '"%"."%"')) then return C_ORACLE_OBJECT_TYPE;
	
	when P_MYSQL_DATA_TYPE is NULL                                    then 
	  signal SQLSTATE  '45000' 
	     set MESSAGE_TEXT  = ERROR_MSG;

    when P_MYSQL_DATA_TYPE = 'char'                                   then return
	case		
 	  when P_DATA_TYPE_LENGTH is null                                 then 'longtext'
      when P_DATA_TYPE_LENGTH > C_MEDIUMTEXT_LENGTH                   then 'longtext'
      when P_DATA_TYPE_LENGTH > C_TEXT_LENGTH                         then 'mediumtext'
      when P_DATA_TYPE_LENGTH > C_VARCHAR_LENGTH                      then 'text'
	  when P_DATA_TYPE_LENGTH > C_CHAR_LENGTH                         then 'varchar'
	                                                                  else P_MYSQL_DATA_TYPE
 	end;

    when P_MYSQL_DATA_TYPE = 'varchar'                                then return
	case		
 	  when P_DATA_TYPE_LENGTH is null                                 then 'longtext'
      when P_DATA_TYPE_LENGTH > C_MEDIUMTEXT_LENGTH                   then 'longtext'
      when P_DATA_TYPE_LENGTH > C_TEXT_LENGTH                         then 'mediumtext'
      when P_DATA_TYPE_LENGTH > C_VARCHAR_LENGTH                      then 'text'
	                                                                  else P_MYSQL_DATA_TYPE
 	end;

    when P_MYSQL_DATA_TYPE in (
      'longtext',
	  'mediumtext',
      'text',
      'tinytext'
	)                                                                 then return
	case		
 	  when P_DATA_TYPE_LENGTH is null                                 then 'longtext'
      when P_DATA_TYPE_LENGTH > C_MEDIUMTEXT_LENGTH                   then 'longtext'
      when P_DATA_TYPE_LENGTH > C_TEXT_LENGTH                         then 'mediumtext'
      when P_DATA_TYPE_LENGTH > C_TINYTEXT_LENGTH                     then 'text'
      when P_DATA_TYPE_LENGTH < C_VARCHAR_LENGTH                      then 'varchar'
                                                                      else P_MYSQL_DATA_TYPE
 	end;

    when P_MYSQL_DATA_TYPE = 'binary'                                 then return
 	case		
      when P_DATA_TYPE_LENGTH is null                                 then 'longblob'
      when P_DATA_TYPE_LENGTH > C_MEDIUMBLOB_LENGTH                   then 'longblob'
      when P_DATA_TYPE_LENGTH > C_BLOB_LENGTH                         then 'mediumblob'
      when P_DATA_TYPE_LENGTH > C_VARBINARY_LENGTH                    then 'blob'
	  when P_DATA_TYPE_LENGTH > C_BINARY_LENGTH                       then 'varbinary'
	                                                                  else P_MYSQL_DATA_TYPE
 	end;

    when P_MYSQL_DATA_TYPE = 'varbinary'                              then return
 	case		
      when P_DATA_TYPE_LENGTH is null                                 then 'longblob'
      when P_DATA_TYPE_LENGTH > C_MEDIUMBLOB_LENGTH                   then 'longblob'
      when P_DATA_TYPE_LENGTH > C_BLOB_LENGTH                         then 'mediumblob'
      when P_DATA_TYPE_LENGTH > C_VARBINARY_LENGTH                    then 'blob'
	                                                                  else P_MYSQL_DATA_TYPE
 	end;

    when P_MYSQL_DATA_TYPE in (
	  'longblob',
	  'mediumblob',
      'blob',
      'tinyblob'
	)                                                                 then return
    case		
      when P_DATA_TYPE_LENGTH is null                                 then 'longblob'
      when P_DATA_TYPE_LENGTH > C_MEDIUMBLOB_LENGTH                   then 'longblob'
      when P_DATA_TYPE_LENGTH > C_BLOB_LENGTH                         then 'mediumblob'
      when P_DATA_TYPE_LENGTH > C_VARBINARY_LENGTH                    then 'blob'
      when P_DATA_TYPE_LENGTH < C_VARBINARY_LENGTH                    then 'varbinary'
	                                                                  else P_MYSQL_DATA_TYPE
 	end;

    when P_MYSQL_DATA_TYPE = 'decimal'                                then return 
	case 
      when P_DATA_TYPE_LENGTH is NULL                                 then C_UNBOUNDED_NUMERIC 
	  when ((P_DATA_TYPE_LENGTH > C_NUMERIC_PRECISION) 
	   and (P_DATA_TYPE_SCALE  > C_NUMERIC_SCALE))                    then C_UNBOUNDED_NUMERIC
	  when P_DATA_TYPE_LENGTH > C_NUMERIC_PRECISION                   then concat(P_MYSQL_DATA_TYPE,'(',cast(C_NUMERIC_PRECISION as CHAR),',',cast(P_DATA_TYPE_SCALE as CHAR),')')
	   when P_DATA_TYPE_SCALE > C_NUMERIC_SCALE                       then concat(P_MYSQL_DATA_TYPE,'(',cast(P_DATA_TYPE_LENGTH as CHAR),',',cast(C_NUMERIC_SCALE as CHAR),')')
	                                                                  else P_MYSQL_DATA_TYPE 
	end;
    
	when P_MYSQL_DATA_TYPE in (
      'time',
	  'datetime'
    )                                                                 then return 
	case 
	  when P_DATA_TYPE_LENGTH > 6                                     then concat(P_MYSQL_DATA_TYPE,'(6)')
		                                                              else P_MYSQL_DATA_TYPE 
    end;

    when P_MYSQL_DATA_TYPE = 'timestamp'                              then return
	  -- MySQL Timestamp limited to Unix EPOCH date range. Map to datetime when data comes from other sources.
	case 
	  when P_VENDOR in('MySQL','MariaDB')                             then P_MYSQL_DATA_TYPE
	  when P_DATA_TYPE_LENGTH > 6                                     then 'datetime(6)'
		                                                              else 'datetime'
    end;

    when P_MYSQL_DATA_TYPE in (
      'bit',
	  'bit varying'
	)                                                                 then return
    case  
	  when P_DATA_TYPE_LENGTH is null                                 then C_LARGEST_VARCHAR_TYPE
      when P_DATA_TYPE_LENGTH > C_MEDIUMTEXT_LENGTH                   then 'longtext'
      when P_DATA_TYPE_LENGTH > C_TEXT_LENGTH                         then 'mediumtext'
      when P_DATA_TYPE_LENGTH > C_VARCHAR_LENGTH                      then 'text'
	  when P_DATA_TYPE_LENGTH > 64                                    then 'varchar'
	                                                                  else P_MYSQL_DATA_TYPE
 	end;
	when P_MYSQL_DATA_TYPE = 'set'                                    then return 'json';
	when P_MYSQL_DATA_TYPE = 'enum'                                   then return 'varchar(512)';
                                                                      else return P_MYSQL_DATA_TYPE;
  end case;															
end;
$$
--
DELIMITER ;
--  