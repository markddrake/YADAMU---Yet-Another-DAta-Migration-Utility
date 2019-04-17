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
  
    // super()
    this.yadamu        =  new Yadamu('YADAMU Tester'); 
    this.config        = require(path.resolve(this.yadamu.getParameters().CONFIG))
    this.connections   = require(path.resolve(this.config.connections))
    this.parsingMethod = CLARINET;
    
    
    // Expand environemnt variables in path using regex.
    this.ros = this.config.outputFile ? fs.createWriteStream(path.resolve(this.config.outputFile.replace(/%([^%]+)%/g, (_,n) => process.env[n]))) : this.yadamu.getLogWriter();
    this.fc = new FileCompare(this.yadamu,this.ros);
    
    
  }
    
  getDatabaseInterface(db) {
  
    let dbi = undefined
    switch (db) {
      case "oracle18c" :
      case "oracle18" :
      case "oracle12c" :
      case "oracleXE" :          
        dbi = new OracleCompare(this.yadamu,this.ros)
        break;
      case "postgres" :
        dbi = new PostgresCompare(this.yadamu,this.ros)
        break;
      case "mssql" :
        dbi = new MsSQLCompare(this.yadamu,this.ros)
        break;
      case "mysql" :
        dbi = new MySQLCompare(this.yadamu,this.ros)
        break;
      case "mariadb" :
        dbi = new MariadbCompare(this.yadamu,this.ros)
        break;
      case "file" :
        dbi = new FileCompare(this.yadamu,this.ros)
        break;
      default:   
        console.log('Invalid Database: ',db);  
      }      
      return dbi;
  }
  
  configureDatabaseInterface(db,role,target,connection) {

    const dbi = this.getDatabaseInterface(db)
    
    const schema = db === "mssql" ? target.schema : target
    const dbParameters = {[role]:schema}
  
    const dbConnection = Object.assign({},connection )
    dbi.updateSettings(dbParameters,dbConnection,role,target)
    this.yadamu.overwriteParameters(dbParameters);
    dbi.setConnectionProperties(dbConnection);
    return dbi;    
  }
  
  async recreateSchema(db,schema,connection) {

     const dbi = this.getDatabaseInterface(db);
     dbi.setConnectionProperties(connection);
     await dbi.initialize();
     await dbi.recreateSchema(schema,connection.PASSWORD);
     await dbi.finalize();
  
  }
  
  async compareSchemas(dbi,source,target,timings) {
      
     await dbi.initialize();
     await dbi.report(source,target,timings);
     await dbi.finalize();
     
  }
  
  async doImport(dbi,file) {
     
    switch (this.parsingMethod)  {
      case CLARINET :
        return this.yadamu.doImport(dbi);
      case RDBMS :
        return this.yadamu.doServerImport(dbi,file);
      default:
        return this.yadamu.doImport(dbi);
    }
    
  }
      
  async fileRoundtrip(target,parameters,sourceFile,db1,targetFile1,db2,targetFile2) {
      
      let dbi
      let dbUser
      
      const testRoot = 'work' + path.sep + target + path.sep;
      
      const timings = []
      const source = 'file';
      const testParameters = parameters ? parameters : {}

      this.yadamu.overwriteParameters(testParameters);    
      
      this.yadamu.getStatus().warningRaised = false;
      this.yadamu.getStatus().errorRaised = false;
      this.yadamu.getStatus().startTime = new Date().getTime()

      this.yadamu.parameters.FILE = sourceFile
      dbUser = this.getDatabaseUser(target,db1)     
      dbi = this.configureDatabaseInterface(target,'TOUSER',dbUser,this.connections[target]);     
      await this.recreateSchema(target,dbUser,this.connections[target]);
      
      let startTime = new Date().getTime();
      timings[0] = await this.doImport(dbi,sourceFile);
      let elapsedTime = new Date().getTime() - startTime;
      let targetDescription = target === 'mssql' ? `${dbUser.schema}"."${dbUser.owner}` : dbUser      
      this.printResults(`"${source}"://"${sourceFile}"`,`"${target}"://"${targetDescription}"`,elapsedTime)

      this.yadamu.parameters.FILE = testRoot + targetFile1
      dbi = this.configureDatabaseInterface(target,'OWNER',dbUser,this.connections[target]);     
      startTime = new Date().getTime()
      timings[1] = await this.yadamu.doExport(dbi);
      elapsedTime = new Date().getTime() - startTime;

      let sourceDescription = targetDescription;
      this.printResults(`"${target}"://"${sourceDescription}"`,`"${source}"://"${targetFile1}"`,elapsedTime)

      dbUser = this.getDatabaseUser(target,db2)     
      dbi = this.configureDatabaseInterface(target,'TOUSER',dbUser,this.connections[target]);     
      await this.recreateSchema(target,dbUser,this.connections[target]);

      startTime = new Date().getTime();
      timings[2] = await this.doImport(dbi,testRoot + targetFile1);
      elapsedTime = new Date().getTime() - startTime;
      targetDescription = target === 'mssql' ? `${dbUser.schema}"."${dbUser.owner}` : dbUser      
      this.printResults(`"${source}"://"${targetFile1}"`,`"${target}"://"${targetDescription}"`,elapsedTime)

      this.yadamu.parameters.FILE = testRoot + targetFile2
      dbi = this.configureDatabaseInterface(target,'OWNER',dbUser,this.connections[target]);     
      startTime = new Date().getTime()
      timings[3] = await this.yadamu.doExport(dbi);
      elapsedTime = new Date().getTime() - startTime;
       
      sourceDescription = targetDescription;
      this.printResults(`"${target}"://"${sourceDescription}"`,`"${source}"://"${targetFile2}"`,elapsedTime)
      
      await this.compareSchemas(dbi,db1,db2,timings[2]);
      await this.fc.report(sourceFile, testRoot + targetFile1, testRoot + targetFile2,timings);    
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
        owner  = this.getDatabaseUser(source,steps[0])  
        toUser = this.getDatabaseUser(source,steps[2])  
        sourceDB = this.configureDatabaseInterface(source,'OWNER',owner,this.connections[source]);
        targetDB = this.configureDatabaseInterface(source,'TOUSER',toUser,this.connections[source]);
        timings = await this.yadamu.pumpData(sourceDB,targetDB);
      }

      tcParameters = Object.assign({},parameters)
      if (clone === true) {
        tcParameters.MODE = 'DDL_AND_DATA';
      } 
      this.yadamu.overwriteParameters(tcParameters);
      
      const sourceSchema = this.getDatabaseUser(source,steps[0])  
      let targetSchema = this.getDatabaseUser(target,steps[1])  
      await this.recreateSchema(target,targetSchema,this.connections[target]);
      sourceDescription = source === 'mssql' ? `${sourceSchema.schema}"."${sourceSchema.owner}` : sourceSchema
      targetDescription = target === 'mssql' ? `${targetSchema.schema}"."${targetSchema.owner}` : targetSchema
      sourceDB = this.configureDatabaseInterface(source,'OWNER',sourceSchema,this.connections[source]);
      targetDB = this.configureDatabaseInterface(target,'TOUSER',targetSchema,this.connections[target]);
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
        targetSchema = this.getDatabaseUser(source,steps[2])  
        await this.recreateSchema(source,targetSchema,this.connections[source]);
        sourceDescription = target === 'mssql' ? `${owner.schema}"."${owner.owner}` : owner
        targetDescription = source === 'mssql' ? `${targetSchema.schema}"."${targetSchema.owner}` : targetSchema
        sourceDB = this.configureDatabaseInterface(target,'OWNER',owner,this.connections[target]);
        targetDB = this.configureDatabaseInterface(source,'TOUSER',targetSchema,this.connections[source]);
        startTime = new Date().getTime();
        timings = await this.yadamu.pumpData(sourceDB,targetDB);
        elapsedTime = new Date().getTime() - startTime
        this.printResults(`"${target}"://"${sourceDescription}"`,`"${source}"://"${targetDescription}"`,elapsedTime)
      }
       
      sourceDB = this.configureDatabaseInterface(source,'OWNER',sourceSchema,this.connections[source]);
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
    
  getDatabaseUser(db,user) {
    switch (db) {
      case "mssql":
        if (user.owner === undefined) {
          user.owner = "dbo"
        }
        return user;
      default:
        if ((user.owner !== undefined) && (user.owner !== 'dbo')) {
          return user.owner;
        }
        return user.schema;
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
      
      this.yadamu.getStatus().warningRaised = false;
      this.yadamu.getStatus().errorRaised = false;
      this.yadamu.getStatus().startTime = new Date().getTime()     

      const testParameters = parameters ? parameters : {}
      
      if (source === 'file') {
        const file = directory ? directory + path.sep + sourceInfo : sourceInfo
        testParameters.FILE = file
        sourceDescription = file;
        this.yadamu.overwriteParameters(testParameters);
        sourceDB = new FileReader(this.yadamu);
      }
      else {
        const owner = this.getDatabaseUser(source,sourceInfo)
        sourceDescription = source === 'mssql' ? `${owner.schema}"."${owner.owner}` : owner
        sourceDB = this.configureDatabaseInterface(source,'OWNER',owner,this.connections[source]);
      }

      const owner = this.getDatabaseUser(target,targetInfo)

      if (target === 'file') {
        const file = directory ? directory + path.sep + targetInfo : targetInfo
        testParameters.FILE = file
        targetDescription = file;
      }
      else {
        targetDescription = target === 'mssql' ? `${owner.schema}"."${owner.owner}` : owner
      }
      
      targetDB = this.configureDatabaseInterface(target,'TOUSER',owner,this.connections[target]);
      this.yadamu.overwriteParameters(testParameters);
      
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