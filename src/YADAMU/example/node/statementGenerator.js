"use strict";

class StatementGenerator {
  
  constructor(dbi, targetSchema, metadata, yadamuLogger) { 
    this.dbi = dbi;
    this.targetSchema = targetSchema
    this.metadata = metadata
    this.yadamuLogger = yadamuLogger;
  }
  

  async generateStatementCache () {    

    /*
    **
    ** Gnerates a statementCache object from the contents of the metadata objects.	
	** 
	** The typical implementation uses a stored procedure to perform the transformation, although there is no reason not to do it with clients-side Javascript.
	**
	*/
    
    const sqlStatement = `CALL STORED PROEDCURE`
    let statementCache = await this.dbi.executeSQL(sqlStatement,[{metadata : this.metadata}, this.targetSchema, this.dbi.INBOUND_SPATIAL_FORMAT])
    if (statementCache === null) {
      statementCache = {}
    }
    return statementCache;
  }
}

module.exports = StatementGenerator;