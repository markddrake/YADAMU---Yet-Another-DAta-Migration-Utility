"use strict"

const yadamuQA = require('../../src/YADAMU_QA/common/node/yadamuQA.js');
const tasks = require('../../qa/regression/tasks.json');

function main() {

  const vendors = ["oracle","mssql","postgres","mysql","mariadb","mongo","snowflake"]
	
  const QA = new yadamuQA();
  Object.keys(tasks).forEach((taskName) => {
	 if (!Array.isArray(tasks[taskName])) {
       const task = tasks[taskName]
       console.log('Task:',taskName,task.vendor,task.source)
	   vendors.forEach((vendor) => {
		 console.log('SourceMapping:',vendor,QA.getSourceMapping(vendor,task))
		 console.log('TargetMapping:',vendor,QA.getTargetMapping(vendor,task,'1'))
	   })
	 }
  })
  
}

main()