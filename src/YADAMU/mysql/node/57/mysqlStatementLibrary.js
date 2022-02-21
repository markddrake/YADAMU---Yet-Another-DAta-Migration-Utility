"use strict" 

import YadamuConstants from '../../../common/yadamuConstants.js';
import DefaultStatmentLibrary from '../mysqlStatementLibrary.js'

class MySQLStatementLibrary extends DefaultStatmentLibrary {   

  // Until we have static constants

  static get SQL_INFORMATION_SCHEMA_FROM_CLAUSE()  { return _SQL_INFORMATION_SCHEMA_FROM_CLAUSE }
   
  get SQL_INFORMATION_SCHEMA_FROM_CLAUSE() { return MySQLStatementLibrary.SQL_INFORMATION_SCHEMA_FROM_CLAUSE }

  constructor(dbi) {
    super(dbi)
  }
}

export { MySQLStatementLibrary  as default }

/*
**
** During testing on 5.7 it appeared tha that is is possible for the Information Schema to get corrupted
** In the corrupt state some table contains duplicate entires for each of the columns in the table.
** 
** This version of the from clause will workaround the problem if the Information schema is corrupt.
** 
*/   


const _SQL_INFORMATION_SCHEMA_FROM_CLAUSE  = 
`   from (
     select distinct c.table_catalog, c.table_schema, c.table_name,column_name,ordinal_position,data_type,column_type,character_maximum_length,numeric_precision,numeric_scale,datetime_precision
       from information_schema.columns c, information_schema.tables t
       where t.table_name = c.table_name 
         and c.extra <> 'VIRTUAL GENERATED'
         and t.table_schema = c.table_schema
         and t.table_type = 'BASE TABLE'
         and t.table_schema = ?
   ) c
  group by c.table_schema, c.table_name`;

}  
