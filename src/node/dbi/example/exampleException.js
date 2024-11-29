
import {
  DatabaseError
}                    from '../../core/yadamuException.js'

class ExampleError extends DatabaseError {
  
  constructor(dbi,cause,stack,sql) {
    super(dbi,cause,stack,sql);
  }

}

export { ExampleError as default }