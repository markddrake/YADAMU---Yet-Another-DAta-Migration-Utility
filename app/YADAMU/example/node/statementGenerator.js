"use strict";

class StatementGenerator {
  
  constructor(dbi, targetSchema, metadata, spatialFormat) {
    
    this.dbi = dbi;
    this.targetSchema = targetSchema
    this.metadata = metadata
    this.spatialFormat = spatialFormat
  }
  

  async generateStatementCache (executeDDL, vendor) {    

    /*
    **
    ** Gnerates a statementCache object from the contents of the metadata objects.	
	** 
	** The typical implementation uses a stored procedure to perform the transformation, although there is no reason not to do it with clients-side Javascript.
	**
	*/
    
    const sqlStatement = `CALL STORED PROEDCURE`
    let statementCache = await this.dbi.executeSQL(sqlStatement,[{metadata : this.metadata}, this.targetSchema, this.spatialFormat])
    if (statementCache === null) {
      statementCache = {}
    }
    else {	  	  
      if (executeDDL === true) {
        await this.dbi.executeDDL(ddlStatements);
      }
    }
    return statementCache;
  }
}

module.exports = StatementGenerator;