"use strict"

import {DatabaseError} from '../../core/yadamuException.js'

class ExampleError extends DatabaseError {
  
  constructor(cause,stack,sql) {
    super(cause,stack,sql);
  }

}

export { ExampleError as default }