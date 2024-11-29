
import {DatabaseError} from '../../core/yadamuException.js'


class TeradataError extends DatabaseError {

  constructor(dbi,cause,stack,sql) {
	super(dbi,cause,stack,sql);
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