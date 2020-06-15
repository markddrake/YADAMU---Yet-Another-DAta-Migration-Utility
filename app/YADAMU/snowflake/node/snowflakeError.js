"use strict"

const {DatabaseError} = require('../../common/yadamuError.js')

class SnowflakeError extends DatabaseError {
  
  constructor(cause,stack,sql) {
    super(cause,stack,sql);
  }

}

module.exports = SnowflakeError