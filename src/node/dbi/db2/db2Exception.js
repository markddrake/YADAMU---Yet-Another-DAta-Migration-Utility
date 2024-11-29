
import {
  DatabaseError
}                    from '../../core/yadamuException.js'

import DB2Constants  from './db2Constants.js'

class DB2Error extends DatabaseError {
  
  constructor(dbi,cause,stack,sql) {
	const err = new Error();
    Object.assign(err,cause)
	err.stack = stack
	super(dbi,err,stack,sql);
  }
  
  lostConnection() {
	return ((this.cause.sqlcode && DB2Constants.LOST_CONNECTION_ERROR.includes(this.cause.sqlcode)) || (this.cause.state && DB2Constants.CLOSED_CONNECTION_ERROR.includes(this.cause.state)))
  }

}

export { DB2Error as default }