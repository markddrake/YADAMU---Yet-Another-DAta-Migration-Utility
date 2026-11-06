
import {DatabaseError} from '../../core/yadamuException.js'


class TeradataError extends DatabaseError {

  constructor(dbi,cause,stack,sql) {
	super(dbi,cause,stack,sql);
  }

  /*
  **
  ** teradatasql reports the error code and SQLState only in the message, e.g.
  **   [Teradata SQL Driver] [Error 301] [SQLState 08S01] Lost connection to the Teradata Database
  ** and every later call on that connection fails with "sql: connection is already closed".
  ** A server that is down or restarting fails to connect with [Error 444] [SQLState 08001].
  **
  */

  lostConnection() {
	const message = this.cause?.message || ''
	return message.includes('[SQLState 08S01]') || message.includes('connection is already closed')
  }

  serverUnavailable() {
	return (this.cause?.message || '').includes('[SQLState 08001]')
  }

  static recreateTeradataError(e) {
	 const teradataError = new TeradataError(e._DRIVER_ID,e.cause,e.stack,e.sql) 
	 Object.assign(teradataError,e)
	 return teradataError
  }
}

export { 
  TeradataError 
}