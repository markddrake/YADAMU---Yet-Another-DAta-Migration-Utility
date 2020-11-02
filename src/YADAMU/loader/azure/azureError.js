"use strict"

const {DatabaseError} = require('../../common/yadamuError.js')

class AzureError extends DatabaseError {
  
  constructor(cause,stack,sql) {
    super(cause,stack,sql);
  }

}

module.exports = AzureError