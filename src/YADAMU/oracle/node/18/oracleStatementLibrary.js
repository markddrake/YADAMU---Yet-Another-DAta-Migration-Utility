"use strict" 

const YadamuConstants = require('../../../common/yadamuConstants.js');
const DefaultStatmentLibrary = require('../oracleStatementLibrary.js')

class OracleStatementLibrary extends DefaultStatmentLibrary{

  static get DB_VERSION()                     { return 18 }
  static get SQL_GET_DLL_STATEMENTS()         { return _SQL_GET_DLL_STATEMENTS }
  
  constructor(dbi) {
    super(dbi)
  }

}
 
module.exports = OracleStatementLibrary

const _SQL_GET_DLL_STATEMENTS   = `select COLUMN_VALUE JSON from TABLE(YADAMU_EXPORT_DDL.FETCH_DDL_STATEMENTS(:schema))`;

  
