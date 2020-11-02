"use strict" 

const AWSS3DBI = require('../../../YADAMU//loader/awsS3/awsS3DBI.js');

class AWSS3QA extends AWSS3DBI {
  
  async recreateSchema() {
	await this.cloudService.deleteFolder(this.IMPORT_FOLDER)
  }
	
  constructor(yadamu) {
    super(yadamu)
  }

  async initialize() {
	await super.initialize();
	if (this.options.recreateSchema === true) {
		await this.recreateSchema();
	}
  }

  setConnectionProperties(connectionProperties) {
    if (connectionProperties.hasOwnProperty('yadamuOptions')) {
	  Object.assign(this.s3Options,connectionProperties.yadamuOptions)
	  delete connectionProperties.yadamuOptions
	}
    super.setConnectionProperties(connectionProperties)
  }
  
}
module.exports = AWSS3QA