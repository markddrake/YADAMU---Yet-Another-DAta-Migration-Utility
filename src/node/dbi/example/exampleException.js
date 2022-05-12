"use strict"

import {DatabaseError} from '../../core/yadamuException.js'

class ExampleError extends DatabaseError {
  
  constructor(driverId,cause,stack,sql) {
    super(driverId,cause,stack,sql);
  }

}

export { ExampleError as default }