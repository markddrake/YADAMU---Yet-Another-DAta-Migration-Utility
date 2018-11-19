const DBFileLoader = require('./dbFileLoader');

Class StagingTable

  constructor(dbConn,tableSpecification,importFilePath,status) {   

  this.request = new sql.Request(dbConn);    
  this.table = tableSpecification;
  this.filePath = importFilePath;
  this.status = status;

  async function dropStagingTable() {
    try {
      const statement = `drop table if exists "${this.table.tableName}"`;
      const results = await this.request.batch(statement)
      return results;
    } catch (e) {}
  }
    
  async function createStagingTable() {
    const statement = `create table "${this.table.tableName}" ("${this.table.column_name}" NVARCHAR(MAX))`;
    const results = await this.request.batch(statement)
    return results;
  } 

  async function initializeStagingTable() {
    const statement = `insert into "${this.table.table_name}" values ('')`;
    const results = await request.batch(statement)
    return results;
  } 
  
  async function uploadFile() {
      
    const statement = `update "${stagingTable.table_name}" set "${stagingTable.column_name}" .write(@data,null,null)`;
     
    const inputStream = fs.createReadStream(dumpFilePath);
    const loader = new DBFileLoader(request,statement);
  
    let results = await dropStagingTable(request,stagingTable);
    results = await createStagingTable(request,stagingTable);
    results = await initializeStagingTable(request,stagingTable);

    let startTime;
    return new Promise(function(resolve, reject) {
	  loader.on('finish',function(chunk) {resolve(new Date().getTime() - startTime)})
	  inputStream.on('error',function(err){reject(err)});
	  loader.on('error',function(err){reject(err)});
	  startTime = new Date().getTime();
      inputStream.pipe(loader);
    })

  }
}
 
export.StagingTable = StagingTable;

