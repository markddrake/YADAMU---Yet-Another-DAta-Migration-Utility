"use strict"

const {DatabaseError} = require('../../common/yadamuError.js')

class S3Error extends DatabaseError {
  
  constructor(cause,stack,sql) {
    super(cause,stack,sql);
  }

}

module.exports = S3Error