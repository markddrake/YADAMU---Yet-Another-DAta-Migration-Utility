"use strict" 
const path = require('path');

const Yadamu = require('./yadamu.js');
const YadamuLibrary = require('./yadamuLibrary.js');
const YadamuLogger = require('./yadamuLogger.js');

class DBCopy {

  constructor() {  
   this.configFilePath = 'yadamuConfiguration.json';
     
    process.argv.forEach(function (arg) {
      if (arg.indexOf('=') > -1) {
        const parameterName = arg.substring(0,arg.indexOf('='));
        const parameterValue = arg.substring(arg.indexOf('=')+1);
        switch (parameterName.toUpperCase()) {
          case 'CONFIG':
          case '--CONFIG':
            this.configFilePath =  parameterValue;
            break;
          default:
            // console.log(`${new Date().toISOString()}[Yadamu]: Unknown parameter: "${parameterName}".`)          
        }
      }
    },this)
    this.configuration = require(path.resolve(this.configFilePath))
    this.yadamuLogger = new YadamuLogger( this.configuration.outputFile ? fs.createWriteStream(path.resolve(this.configuration.outputFile.replace(/%([^%]+)%/g, (_,n) => process.env[n]))) : process.stdout)

  }
  
  getDatabaseInterface(driver,yadamu) {
    
    let dbi = undefined
    
    switch (driver) {
      case "oracle"  : 
        const OracleDBI = require('../oracle/node/oracleDBI.js');
        dbi = new OracleDBI(yadamu)
        break;
      case "postgres" :
        const PostgresDBI = require('../postgres/node/postgresDBI.js');
        dbi = new PostgresDBI(yadamu)
        break;
      case "mssql" :
        const MsSQLDBI = require('../mssql/node/msSQLDBI.js');
        dbi = new MsSQLDBI(yadamu)
        break;
      case "mysql" :
        const MySQLDBI = require('../mysql/node/mySQLDBI.js');
        dbi = new MySQLDBI(yadamu)
        break;
      case "mariadb" :
        const MariaDBI = require('../mariadb/node/mariaDBI.js');
        dbi = new MariaDBI(yadamu)
        break;
      case "mongodb" :
        const MongoDBI = require('../mongob/node/mongoDBI.js');
        dbi = new MongoDBI(yadamu)
        break;
      case "snowflake" :
        const SnowflakeDBI = require('../snowflake/node/snowflakeDBI.js');
        dbi = new SnowflakeDBI(yadamu)
        break;
      case "file" :
        dbi = new FileCompare(yadamu)
        break;
      default:   
        this.yadamuLogger.log([`${this.constructor.name}.getDatabaseInterface()`,`${driver}`],`Unknown Database.`);  
      }      
      return dbi;
  }
  
  getOwner(vendor,schema) {
    
     return vendor === 'mssql' ? schema.mssql.owner : (vendor === 'snowflake' ? schema.snowflake.schema : schema.schema)
     
  }
  
  getDescription(db,connectionName,schemaInfo) {
    return `"${connectionName}"://"${db === 'mssql' ? `${schemaInfo.mssql.database}"."${schemaInfo.mssql.owner}` : schemaInfo.schema}"`
  }
  
    
  async runJobs() {
  
    const startTime = new Date().getTime();
    for (const job of this.configuration.jobs) {
      // Initialize constructor parameters with values from configuration file
      const jobParameters = Object.assign({} , this.configuration.parameters ? this.configuration.parameters : {})
      // Merge job specific parameters
      Object.assign(jobParameters,job.parameters ? job.parameters : {})
    
      const sourceSchema = this.configuration.schemas[job.source.schema]
      const sourceConnection = this.configuration.connections[job.source.connection]
      const sourceDatabase =  Object.keys(sourceConnection)[0];
      const sourceDescription = this.getDescription(sourceDatabase,job.source.connection,sourceSchema)

      const targetSchema = this.configuration.schemas[job.target.schema]
      const targetConnection = this.configuration.connections[job.target.connection]
      const targetDatabase =  Object.keys(targetConnection)[0];
      const targetDescription = this.getDescription(targetDatabase,job.target.connection,targetSchema)
  
      const yadamu = new Yadamu('DBCopy',jobParameters);
      
      const sourceDBI = this.getDatabaseInterface(sourceDatabase,yadamu)
      sourceDBI.setConnectionProperties(sourceConnection[sourceDatabase]);
      sourceDBI.parameters.FROM_USER = this.getOwner(sourceDatabase,sourceSchema);
      switch (sourceDatabase) {
         case 'mssql':
           sourceDBI.parameters.MSSQL_SCHEMA_DB = sourceSchema.mssql.database
           break;
         default:
      }

      const targetDBI = this.getDatabaseInterface(targetDatabase,yadamu)    
      targetDBI.setConnectionProperties(targetConnection[targetDatabase]);
      targetDBI.parameters.TO_USER = this.getOwner(targetDatabase,targetSchema);
      switch (targetDatabase) {
         case 'mssql':
           targetDBI.parameters.MSSQL_SCHEMA_DB = targetSchema.mssql.database
           break;
         default:
      }
      
      await yadamu.doCopy(sourceDBI,targetDBI);      
      this.yadamuLogger.log([`${this.constructor.name}`,'doCopy'],`Operation complete. Source:[${sourceDescription}]. Target:[${targetDescription}].`);
    }
    const elapsedTime = new Date().getTime() - startTime;
    this.yadamuLogger.log([`${this.constructor.name}`,'TEST'],`Operation complete: Configuration:"${this.configFilePath}". Elapsed Time: ${YadamuLibrary.stringifyDuration(elapsedTime)}s.`);
  }

  validateConfiguration() {
  }

  loadConfiguration() {
      
    if (typeof this.configuration.connections === "string") {
      this.configuration.connections = require(path.resolve(this.configuration.connections))
    }
    if (typeof this.configuration.schemas === "string") {
      this.configuration.schemas = require(path.resolve(this.configuration.schemas))
    }
    if (typeof this.configuration.parameters === "string") {
      this.configuration.parameters = require(path.resolve(this.configuration.parameters))
    }
    if (typeof this.configuration.jobs === "string") {
      this.configuration.jobs = require(path.resolve(this.configuration.jobs))
    }
    
    this.validateConfiguration()
    
  }
}  

function exit() {
    
  console.log(`[ERROR][DBCopy.exit()]: Forced exit.`);
  process.exit();
  
}

async function main() {
    
  try {
    const dbCopy = new DBCopy();
    dbCopy.loadConfiguration();
    await dbCopy.runJobs();
  } catch (e) {
    console.log(`[ERROR][DBCopy.main()]: Unexpected Terminal Exception`);
    console.log(`${(e.stack ? e.stack : e)}`)
    console.log(`[ERROR][DBCopy.main()]: Operation failed.`);
    // setTimeout(exit,1000);
    process.exit();
  }
}

main();