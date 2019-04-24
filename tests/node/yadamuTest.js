"use strict" 
const fs = require('fs');
const path = require('path');
const Transform = require('stream').Transform;

const Yadamu = require('../../common/yadamu.js').Yadamu;

const OracleCompare = require('./oracleCompare.js');
const MsSQLCompare = require('./mssqlCompare.js');
const MySQLCompare = require('./mysqlCompare.js');
const MariadbCompare = require('./mariadbCompare.js');
const PostgresCompare = require('./postgresCompare.js');
const FileCompare = require('./fileCompare.js');

const FileReader = require('../../file/node/fileReader.js');

const CLARINET = 1;
const RDBMS    = 2;

class YadamuTester {


  constructor() {
  
    this.yadamu        = new Yadamu('YADAMU Tester'); 
    this.config        = require(path.resolve(this.yadamu.getParameters().CONFIG))
    this.connections   = require(path.resolve(this.config.connections))
    this.parsingMethod = CLARINET;
       
    // Expand environemnt variables in path using regex.
    this.ros = this.config.outputFile ? fs.createWriteStream(path.resolve(this.config.outputFile.replace(/%([^%]+)%/g, (_,n) => process.env[n]))) : this.yadamu.getLogWriter();
  }
    
  getDatabaseInterface(db) {
  
    let dbi = undefined
    switch (db) {
      case "oracle18c" :
      case "oracle18" :
      case "oracle12c" :
      case "oracleXE" :          
        dbi = new OracleCompare(this.yadamu)
        break;
      case "postgres" :
        dbi = new PostgresCompare(this.yadamu)
        break;
      case "mssql" :
        dbi = new MsSQLCompare(this.yadamu)
        break;
      case "mysql" :
        dbi = new MySQLCompare(this.yadamu)
        break;
      case "mariadb" :
        dbi = new MariadbCompare(this.yadamu)
        break;
      case "file" :
        dbi = new FileCompare(this.yadamu)
        break;
      default:   
        console.log('Invalid Database: ',db);  
      }      
      return dbi;
  }
  
  getTestInterface(db,role,schema,testParameters,testConnection) {

    const parameters = testParameters ? Object.assign({},testParameters) : {}
    const connection = Object.assign({},testConnection)

    this.yadamu.reset();
    const dbi = this.getDatabaseInterface(db)
    
    parameters[role] = (db === "mssql" ? schema.owner : schema)
    dbi.configureTest(this.ros,connection,parameters,schema)
    return dbi;    
  }
  
  async recreateSchema(db,connection,schema) {

     this.yadamu.reset();
     const dbi = this.getDatabaseInterface(db);
     dbi.setConnectionProperties(connection)
     await dbi.initialize();
     await dbi.recreateSchema(schema,connection.PASSWORD);
     await dbi.finalize();
  
  }
    
  async doImport(dbi,file) {
     
    switch (this.parsingMethod)  {
      case CLARINET :
        return this.yadamu.testImport(dbi,file);
      case RDBMS :
        return this.yadamu.doServerImport(dbi);
      default:
        return this.yadamu.testImport(dbi,file);
    }
    
  }
       
  getDatabaseSchema(db,schema) {
      
    switch (db) {
      case "mssql":
        if (schema.owner === undefined) {
          schema.owner = "dbo"
        }
        return schema;
      default:
        if ((schema.owner !== undefined) && (schema.owner !== 'dbo')) {
          return schema.owner;
        }
        return schema.schema;
    }
  } 

  async compareSchemas(dbi,source,target,timings) {

     this.yadamu.reset();      
     await dbi.initialize();
     await dbi.report(source,target,timings);
     await dbi.finalize();
     
  }

  async fileRoundtrip(db,parameters,sourceFile,targetSchema1,targetFile1,targetSchema2,targetFile2) {
      
      const source = 'file';
      const timings = []
      const testRoot = path.join('work',db);

      let dbi
      let dbSchema
        
      dbSchema = this.getDatabaseSchema(db,targetSchema1)     
      await this.recreateSchema(db,this.connections[db],dbSchema);     

      let testParameters = parameters ? Object.assign({},parameters) : {}
      dbi = this.getTestInterface(db,'TOUSER',dbSchema,testParameters,this.connections[db]);     
      
      let startTime = new Date().getTime();
      timings[0] = await this.doImport(dbi,sourceFile);
      let elapsedTime = new Date().getTime() - startTime;

      let targetDescription = db === 'mssql' ? `${dbSchema.schema}"."${dbSchema.owner}` : dbSchema      
      this.printResults(`"${source}"://"${sourceFile}"`,`"${db}"://"${targetDescription}"`,elapsedTime)

      testParameters = parameters ? Object.assign({},parameters) : {}
      dbi = this.getTestInterface(db,'OWNER',dbSchema,testParameters,this.connections[db]);     

      startTime = new Date().getTime()
      timings[1] = await this.yadamu.testExport(dbi,path.join(testRoot,targetFile1));
      elapsedTime = new Date().getTime() - startTime;

      let sourceDescription = targetDescription;
      this.printResults(`"${db}"://"${sourceDescription}"`,`"${source}"://"${targetFile1}"`,elapsedTime)

      testParameters = parameters ? Object.assign({},parameters) : {}
      dbSchema = this.getDatabaseSchema(db,targetSchema2)     
      await this.recreateSchema(db,this.connections[db],dbSchema);     

      testParameters = parameters ? Object.assign({},parameters) : {}
      dbi = this.getTestInterface(db,'TOUSER',dbSchema,testParameters,this.connections[db]);     


      startTime = new Date().getTime();
      timings[2] = await this.doImport(dbi,path.join(testRoot,targetFile1));
      elapsedTime = new Date().getTime() - startTime;
      
      targetDescription = db === 'mssql' ? `${dbSchema.schema}"."${dbSchema.owner}` : dbSchema      
      this.printResults(`"${source}"://"${targetFile1}"`,`"${db}"://"${targetDescription}"`,elapsedTime)

      testParameters = parameters ? Object.assign({},parameters) : {}
      dbi = this.getTestInterface(db,'OWNER',dbSchema,testParameters,this.connections[db]);     

      startTime = new Date().getTime()
      timings[3] = await this.yadamu.testExport(dbi,path.join(testRoot,targetFile2));
      elapsedTime = new Date().getTime() - startTime;
 
      sourceDescription = targetDescription;
      this.printResults(`"${db}"://"${sourceDescription}"`,`"${source}"://"${targetFile2}"`,elapsedTime)
      
      await this.compareSchemas(dbi,targetSchema1,targetSchema2,timings[2]);
      
      const fc = new FileCompare(this.yadamu,this.ros);
      fc.configureTest(this.ros,{},(dbi.parameters.TABLE_MATCHING ? {TABLE_MATCHING : dbi.parameters.TABLE_MATCHING} : {}))
      await fc.report(sourceFile, path.join(testRoot,targetFile1), path.join(testRoot,targetFile2),timings);    
  }

  async databaseRoundtrip(source,target,clone,parameters,steps) {
  
      let importFile
      let exportFile
      
      let toUser
      let owner
    
      let sourceDB
      let targetDB
      let sourceDescription
      let targetDescription     
      
      let startTime
      let elapsedTime
  
      const testParameters = parameters ? parameters : {}
      this.yadamu.overwriteParameters(testParameters);
      
      let timings
      let tcParameters
      
      this.yadamu.getStatus().warningRaised = false;
      this.yadamu.getStatus().errorRaised = false;
      this.yadamu.getStatus().startTime = new Date().getTime()
      
      /*
      **
      ** Clone Mode Logic
      ** 
      ** If source === target Step 0 --> Step 1 DDL_AND_DATA
      **
      ** If source != target   Step 0 --> Step 2 DDL_ONLY. Step 0 --> Step 1 DDL_AND_DATA. Step 1 --> Step 2 DATA_ONLY
      **
      */
      
      
      if ((clone === true) && (source !== target)) {
        const parameters = { "MODE" : "DDL_ONLY" }
        this.yadamu.overwriteParameters(parameters);
        owner  = this.getDatabaseSchema(source,steps[0])  
        toUser = this.getDatabaseSchema(source,steps[2])  
        sourceDB = this.getTestInterface(source,'OWNER',owner,this.connections[source]);
        targetDB = this.getTestInterface(source,'TOUSER',toUser,this.connections[source]);
        timings = await this.yadamu.pumpData(sourceDB,targetDB);
      }

      tcParameters = Object.assign({},parameters)
      if (clone === true) {
        tcParameters.MODE = 'DDL_AND_DATA';
      } 
      this.yadamu.overwriteParameters(tcParameters);
      
      const sourceSchema = this.getDatabaseSchema(source,steps[0])  
      let targetSchema = this.getDatabaseSchema(target,steps[1])  
      await this.recreateSchema(target,this.connections[target],targetSchema);
      sourceDescription = source === 'mssql' ? `${sourceSchema.schema}"."${sourceSchema.owner}` : sourceSchema
      targetDescription = target === 'mssql' ? `${targetSchema.schema}"."${targetSchema.owner}` : targetSchema
      sourceDB = this.getTestInterface(source,'OWNER',sourceSchema,this.connections[source]);
      targetDB = this.getTestInterface(target,'TOUSER',targetSchema,this.connections[target]);
      startTime = new Date().getTime();
      timings = await this.yadamu.pumpData(sourceDB,targetDB);
      elapsedTime = new Date().getTime() - startTime
      this.printResults(`"${source}"://"${sourceDescription}"`,`"${target}"://"${targetDescription}"`,elapsedTime)
     
      if (source !== target) {
        tcParameters = Object.assign({},parameters)
        if (clone === true) {
          tcParameters.MODE = 'DATA_ONLY';
        } 
        this.yadamu.overwriteParameters(tcParameters);
        owner  = toUser  
        targetSchema = this.getDatabaseSchema(source,steps[2])  
        await this.recreateSchema(source,this.connections[source],targetSchema);
        sourceDescription = target === 'mssql' ? `${owner.schema}"."${owner.owner}` : owner
        targetDescription = source === 'mssql' ? `${targetSchema.schema}"."${targetSchema.owner}` : targetSchema
        sourceDB = this.getTestInterface(target,'OWNER',owner,this.connections[target]);
        targetDB = this.getTestInterface(source,'TOUSER',targetSchema,this.connections[source]);
        startTime = new Date().getTime();
        timings = await this.yadamu.pumpData(sourceDB,targetDB);
        elapsedTime = new Date().getTime() - startTime
        this.printResults(`"${target}"://"${sourceDescription}"`,`"${source}"://"${targetDescription}"`,elapsedTime)
      }
       
      sourceDB = this.getTestInterface(source,'OWNER',sourceSchema,this.connections[source]);
      await this.compareSchemas(sourceDB, sourceSchema, targetSchema, timings);
      
  }
  
  printResults(sourceDescription,targetDescription,elapsedTime) {
  
    if (this.ros !== process.stdout) {
      
      const colSizes = [24,128,12]
      let seperatorSize = (colSizes.length * 3) - 1;
      colSizes.forEach(function(size) {
        seperatorSize += size;
      },this);
    
      this.ros.write('\n+' + '-'.repeat(seperatorSize) + '+' + '\n') 
     
      this.ros.write(`| ${'TIMESTAMP'.padEnd(colSizes[0])} |`
                 + ` ${'OPERATION'.padEnd(colSizes[1])} |`
                 + ` ${'ELASPED TIME'.padStart(colSizes[2])} |` 
                 
                 + '\n');
      this.ros.write('+' + '-'.repeat(seperatorSize) + '+' + '\n') 
      
      this.ros.write(`| ${new Date().toISOString().padEnd(colSizes[0])} |`
                 + ` ${(sourceDescription + ' --> ' + targetDescription).padEnd(colSizes[1])} |`
                 + ` ${(elapsedTime.toString()+"ms").padStart(colSizes[2])} |` 
                 + '\n');
                 
      this.ros.write('+' + '-'.repeat(seperatorSize) + '+' + '\n\n') 
    }
    else {
      this.ros.write(`${new Date().toISOString()}[PUMP: Operation complete] SOURCE:[${sourceDescription}]. TARGET:[${targetDescription}]. Elapsed Time: ${elapsedTime}ms.\n`);
    }
  
  }


  async copyContent(source,target,parameters,directory,sourceInfo,targetInfo) {
 
      let pathToFile;
  
      let sourceDB
      let sourceDescription
      let targetDB
      let targetDescription     
      
      let startTime
      let elapsedTime
  
      const timings = []

      this.yadamu.reset();      
      const testParameters = parameters ? parameters : {}
      
      if (source === 'file') {
        const file = directory ? path.join(directory,sourceInfo) : sourceInfo
        sourceDescription = file;
        sourceDB = new FileReader(this.yadamu);
        sourceDB.configureTest({},{FILE : file},this.DEFAULT_PARAMETERS);
      }
      else {
        const dbSchema = this.getDatabaseSchema(source,sourceInfo)
        sourceDescription = source === 'mssql' ? `${dbSchema.schema}"."${dbSchema.owner}` : dbSchema
        sourceDB = this.getTestInterface(source,'OWNER',dbSchema,testParameters,this.connections[source]);
      }

      const dbSchema = this.getDatabaseSchema(target,targetInfo)

      if (target === 'file') {
        const file = directory ? path.join(directory,targetInfo) : targetInfo
        targetDescription = file;
        testParameters.FILE = file
      }
      else {
        targetDescription = target === 'mssql' ? `${dbSchema.schema}"."${dbSchema.owner}` : dbSchema
      }
      
      targetDB = this.getTestInterface(target,'TOUSER',dbSchema,testParameters,this.connections[target]);
      
      
      startTime = new Date().getTime()
      timings[0] = await this.yadamu.pumpData(sourceDB,targetDB);
      elapsedTime = new Date().getTime() - startTime;
      this.printResults(`"${source}"://"${sourceDescription}"`,`"${target}"://"${targetDescription}"`,elapsedTime)
      
  }
    
  async doOperation(target,tc,steps) {
      
    switch (this.config.mode.toUpperCase()) {
      case "EXPORT":
      case "IMPORT":
        if (tc.reverseDirection) {
          await this.copyContent(tc.source,target,tc.parameters,tc.directory,steps[1],steps[0])
        }
        else {
          await this.copyContent(tc.source,target,tc.parameters,tc.directory,steps[0],steps[1])
        }
        break
        break
      case "EXPORTROUNDTRIP":
        await this.fileRoundtrip(target,tc.parameters,steps[0],steps[1],steps[2],steps[3],steps[4]);
        // await this.exportRoundtrip(target,tc,steps);
        break;
      case "DBROUNDTRIP":
        const clone = (this.config.clone && (this.config.clone === true)) 
        await this.databaseRoundtrip(tc.source,target,clone,tc.parameters,steps)
     default:
    }
       
  }
  
  async doOperations(target,tc,operationPath) {
      
    const operations = require(path.resolve(operationPath))
    for (const steps of operations) {
      // await this.doOperation(target,tc,steps)
      await this.doOperation(target,tc,steps)
    }
  }
  
  async runTests() {
 
    const testConfigurationList = this.config.tests
    
    for (const testConfigurationPath of testConfigurationList) {
      const testConfigurations = require(path.resolve(testConfigurationPath));
      for (const tc of testConfigurations) {
        if (tc.parsingMethod) {
          switch (tc.parsingMethod) {
            case "CLARINET" :
              this.parsingMethod = CLARINET;
              break;
            case "RDBMS" :
              this.parsingMethod = RDBMS;
              break;
            default:
              this.parsingMethod = RDBMS;
          }
        }
        for (const target of tc.targets) {
          for (const operationPath of tc.operations) {
            await this.doOperations(target,tc,operationPath)
          }
        }
      }
    }
  
    if (this.ros !== process.stdout) {
      this.ros.close();
    }
  
    this.yadamu.close()
  }
}  
async function main() {
    
  try {
    const yadamuTester = new YadamuTester();
    await yadamuTester.runTests();
  } catch (e) {
    console.log(e);
  }
}

main();