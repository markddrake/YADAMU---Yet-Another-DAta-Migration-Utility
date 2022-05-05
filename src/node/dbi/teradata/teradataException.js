"use strict"

import {DatabaseError} from '../../core/yadamuException.js'


class TeradataError extends DatabaseError {

  constructor(driverId,cause,sql) {
	super(driverId,cause,undefined,sql);
  }

  static recreateTeradataError(e) {
	 const teradataError = new TeradataError(e._DRIVER_ID,e.cause,e.sql) 
	 Object.assign(teradataError,e)
	 return teradataError
  }
}

export { 
  TeradataError 
}