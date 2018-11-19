 class DBFileLoader extends Writable {
     
  constructor(request,statement,options) {
    super(options)
    this.request = request;
    this.statement = statement;
  }
     
  async _write(chunk, encoding, next) {
    try {
      const data = chunk.toString()
      var results = await this.request.input('data',sql.NVARCHAR,data).batch(this.statement);
      next(null,results);
    } catch (e) {
      next(e);
    }   
  } 
}   
 
module.DBFileLoader = DBFileLoader
