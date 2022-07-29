
import YadamuConstants from '../../../lib/yadamuConstants.js';
import DefaultStatmentLibrary from '../oracleStatementLibrary.js'

class OracleStatementLibrary extends DefaultStatmentLibrary{

  static get DATABASE_VERSION()                     { return 18 }
  static get SQL_GET_DLL_STATEMENTS()         { return _SQL_GET_DLL_STATEMENTS }
  
  constructor(dbi) {
    super(dbi)
  }

}
 
export {OracleStatementLibrary as default }

const _SQL_GET_DLL_STATEMENTS   = `select COLUMN_VALUE JSON from TABLE(YADAMU_EXPORT_DDL.FETCH_DDL_STATEMENTS(:schema))`;

  
