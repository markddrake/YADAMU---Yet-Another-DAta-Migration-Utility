"use strict" 

const AzureDBI = require('../../../YADAMU//loader/azure/azureDBI.js');

class AzureQA extends AzureDBI {
  
    
  constructor(yadamu) {
     super(yadamu)
  }
  
}
module.exports = AzureQA