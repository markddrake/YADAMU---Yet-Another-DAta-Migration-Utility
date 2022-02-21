
import YadamuWriter from './yadamuWriter.js';

class PerformanceReporter extends YadamuWriter {

   constructor(dbi,tableInfo,metrics,status,yadamuLogger) {
     super(dbi,tableInfo.tableName,metrics,status,yadamuLogger)
	 this.tableInfo = tableInfo
	 this.displayName = tableInfo.tableName
   }
   
   async doConstruct() {}
   
   async doWrite(obj) {}
   
   async doDestroy(err) {}
	
   async doFinal() {}

}

export { PerformanceReporter as default }