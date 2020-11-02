"use strict"

const {DatabaseError} = require('../../common/yadamuError.js')

class AWSS3Error extends DatabaseError {
  
  constructor(cause,stack,sql) {
    super(cause,stack,sql);
  }

}

module.exports = AWSS3Error