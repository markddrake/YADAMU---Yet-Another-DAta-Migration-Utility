"use strict" 

const S3DBI = require('../../../YADAMU//loader/s3/s3DBI.js');

class S3QA extends S3DBI {
  
    
  constructor(yadamu) {
     super(yadamu)
  }
  
}
module.exports = S3QA