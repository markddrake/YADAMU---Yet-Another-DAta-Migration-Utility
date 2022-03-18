
import YadamuStatementGenerator  from '../base/yadamuStatementGenerator.js'

class ExampleStatementGenerator {
  
  constructor(dbi, vendor, targetSchema, metadata, yadamuLogger) {
    super(dbi, vendor, targetSchema, metadata, yadamuLogger)
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

export { ExampleStatementGenerator as default }