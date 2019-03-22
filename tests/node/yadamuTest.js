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
      case "oracle" :
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
        return this.yadamu.doImport(dbi,file);
      case RDBMS :
        return this.yadamu.doServerImport(dbi,file);
      default:
        return this.yadamu.doImport(dbi,file);
    }
    
  }
      
  async exportRoundtrip(target,tc,steps) {
      
      const outputFolder =  "tests" + path.sep + target + path.sep
      
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
  
      const testParameters = tc.parameters ? tc.parameters : {}
      this.yadamu.overwriteParameters(testParameters);
      
      const timings = []
      
      this.yadamu.getStatus().warningRaised = false;
      this.yadamu.getStatus().errorsRaised = false;
      this.yadamu.getStatus().startTime = new Date().getTime()
      
      importFile = steps[0];
      toUser = this.getDatabaseUser(target,steps[1])

      sourceDescription =importFile
      targetDescription = target === 'mssql' ? `${toUser.schema}"."${toUser.owner}` : toUser
      await this.recreateSchema(target,toUser,this.connections[target]);
      targetDB = this.configureDatabaseInterface(target,'TOUSER',toUser,this.connections[target]);
      startTime = new Date().getTime();
      timings[0] = await this.doImport(targetDB,importFile);
      elapsedTime = new Date().getTime() - startTime;
      this.printResults(`"${tc.source}"://"${sourceDescription}"`,`"${target}"://"${targetDescription}"`,elapsedTime)

      exportFile = outputFolder + steps[2];
      owner =  this.getDatabaseUser(target,steps[1])
      
      this.yadamu.parameters.FILE = exportFile
      this.yadamu.overwriteParameters(testParameters);    
      targetDescription = exportFile
      sourceDescription = target === 'mssql' ? `${owner.schema}"."${owner.owner}` : owner
      sourceDB = this.configureDatabaseInterface(target,'OWNER',owner,this.connections[target]);
      startTime = new Date().getTime()
      timings[1] = await this.yadamu.doExport(targetDB);
      elapsedTime = new Date().getTime() - startTime;
      this.printResults(`"${target}"://"${sourceDescription}"`,`"${tc.source}"://"${targetDescription}"`,elapsedTime)
    
      importFile = outputFolder + steps[2];
      toUser = this.getDatabaseUser(target,steps[3])
      
      sourceDescription = importFile
      targetDescription = target === 'mssql' ? `${toUser.schema}"."${toUser.owner}` : toUser
      await this.recreateSchema(target,toUser,this.connections[target]);
      targetDB = this.configureDatabaseInterface(target,'TOUSER',toUser,this.connections[target]);
      startTime = new Date().getTime();
      timings[2] = await this.doImport(targetDB,importFile) 
      elapsedTime = new Date().getTime() - startTime;
      this.printResults(`"${tc.source}"://"${sourceDescription}"`,`"${target}"://"${targetDescription}"`,elapsedTime)
  
      exportFile = outputFolder + steps[4];
      owner =  this.getDatabaseUser(target,steps[3])

      this.yadamu.parameters.FILE = exportFile
      this.yadamu.overwriteParameters(testParameters);
      targetDescription = exportFile
      sourceDescription = target === 'mssql' ? `${owner.schema}"."${owner.owner}` : owner
      sourceDB = this.configureDatabaseInterface(target,'OWNER',owner,this.connections[target]);
      startTime = new Date().getTime();
      timings[3] = await this.yadamu.doExport(targetDB);
      elapsedTime = new Date().getTime() - startTime;
      this.printResults(`"${target}"://"${sourceDescription}"`,`"${tc.source}"://"${targetDescription}"`,elapsedTime)
      
      await this.compareSchemas(sourceDB,steps[1],steps[3],timings[2]);
      await this.fc.report(steps[0],outputFolder + steps[2],outputFolder + steps[4],timings);
     
    
  }

  async databaseRoundtrip(target,tc,steps,clone) {
  
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
  
      const testParameters = tc.parameters ? tc.parameters : {}
      this.yadamu.overwriteParameters(testParameters);
      
      let timings
      let tcParameters
      
      this.yadamu.getStatus().warningRaised = false;
      this.yadamu.getStatus().errorsRaised = false;
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
      
      
      if ((clone === true) && (tc.source !== target)) {
        const parameters = { "MODE" : "DDL_ONLY" }
        this.yadamu.overwriteParameters(parameters);
        owner  = this.getDatabaseUser(tc.source,steps[0])  
        toUser = this.getDatabaseUser(tc.source,steps[2])  
        sourceDB = this.configureDatabaseInterface(tc.source,'OWNER',owner,this.connections[tc.source]);
        targetDB = this.configureDatabaseInterface(tc.source,'TOUSER',toUser,this.connections[tc.source]);
        timings = await this.yadamu.pumpData(sourceDB,targetDB);
      }

      tcParameters = Object.assign({},tc.parameters)
      if (clone === true) {
        tcParameters.MODE = 'DDL_AND_DATA';
      } 
      this.yadamu.overwriteParameters(tcParameters);
      
      const sourceSchema = this.getDatabaseUser(tc.source,steps[0])  
      let targetSchema = this.getDatabaseUser(target,steps[1])  
      await this.recreateSchema(target,targetSchema,this.connections[target]);
      sourceDescription = tc.source === 'mssql' ? `${sourceSchema.schema}"."${sourceSchema.owner}` : sourceSchema
      targetDescription = target === 'mssql' ? `${targetSchema.schema}"."${targetSchema.owner}` : targetSchema
      sourceDB = this.configureDatabaseInterface(tc.source,'OWNER',sourceSchema,this.connections[tc.source]);
      targetDB = this.configureDatabaseInterface(target,'TOUSER',targetSchema,this.connections[target]);
      startTime = new Date().getTime();
      timings = await this.yadamu.pumpData(sourceDB,targetDB);
      elapsedTime = new Date().getTime() - startTime
      this.printResults(`"${tc.source}"://"${sourceDescription}"`,`"${target}"://"${targetDescription}"`,elapsedTime)
     
      if (tc.source !== target) {
        tcParameters = Object.assign({},tc.parameters)
        if (clone === true) {
          tcParameters.MODE = 'DATA_ONLY';
        } 
        this.yadamu.overwriteParameters(tcParameters);
        owner  = toUser  
        targetSchema = this.getDatabaseUser(tc.source,steps[2])  
        await this.recreateSchema(tc.source,targetSchema,this.connections[tc.source]);
        sourceDescription = target === 'mssql' ? `${owner.schema}"."${owner.owner}` : owner
        targetDescription = tc.source === 'mssql' ? `${targetSchema.schema}"."${targetSchema.owner}` : targetSchema
        sourceDB = this.configureDatabaseInterface(tc.target,'OWNER',owner,this.connections[target]);
        targetDB = this.configureDatabaseInterface(tc.source,'TOUSER',targetSchema,this.connections[tc.source]);
        startTime = new Date().getTime();
        timings = await this.yadamu.pumpData(sourceDB,targetDB);
        elapsedTime = new Date().getTime() - startTime
        this.printResults(`"${target}"://"${sourceDescription}"`,`"${tc.source}"://"${targetDescription}"`,elapsedTime)
      }
       
      sourceDB = this.configureDatabaseInterface(tc.source,'OWNER',sourceSchema,this.connections[tc.source]);
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
  
  
  async exportContent(target,tc,steps) {

      let sourceDB
      let sourceDescription
      let targetDescription     
      
      let startTime
      let elapsedTime
  
      const timings = []
      
      this.yadamu.getStatus().warningRaised = false;
      this.yadamu.getStatus().errorsRaised = false;
      this.yadamu.getStatus().startTime = new Date().getTime()     
      
      const file =  tc.directory ? tc.directory + path.sep + steps[1] : steps[1]
      const owner = this.getDatabaseUser(tc.source,steps[0])
      
      const testParameters = tc.parameters ? tc.parameters : {}
      testParameters.FILE = file
      this.yadamu.overwriteParameters(testParameters);
      targetDescription = file
      sourceDescription = target === 'mssql' ? `${owner.schema}"."${owner.owner}` : owner
      sourceDB = this.configureDatabaseInterface(tc.source,'OWNER',owner,this.connections[tc.source]);
      startTime = new Date().getTime()
      timings[0] = await this.yadamu.doExport(sourceDB);
      elapsedTime = new Date().getTime() - startTime;
      this.printResults(`"${tc.source}"://"${sourceDescription}"`,`"${target}"://"${targetDescription}"`,elapsedTime)
      
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
       
  async importContent(target,tc,steps) {

      let targetDB
      let sourceDescription
      let targetDescription     
      
      let startTime
      let elapsedTime
  
      const timings = []
      
      const file = tc.directory ? tc.directory + path.sep + steps[1] : steps[1]
      const toUser = this.getDatabaseUser(target,steps[0])
      
      this.yadamu.getStatus().warningRaised = false;
      this.yadamu.getStatus().errorsRaised = false;
      this.yadamu.getStatus().startTime = new Date().getTime()     
      
      const testParameters = tc.parameters ? tc.parameters : {}
      this.yadamu.overwriteParameters(testParameters);
      sourceDescription =  file
      targetDescription = target === 'mssql' ? `${toUser.schema}"."${toUser.owner}` : toUser
      await this.recreateSchema(target,toUser,this.connections[target]);
      targetDB = this.configureDatabaseInterface(target,'TOUSER',toUser,this.connections[target]);
      startTime = new Date().getTime()
      timings[0] = await this.yadamu.doImport(targetDB, file);
      elapsedTime = new Date().getTime() - startTime;
      this.printResults(`"${tc.source}"://"${sourceDescription}"`,`"${target}"://"${targetDescription}"`,elapsedTime)
      
  }
  

  async doOperation(target,tc,steps) {
      
    switch (this.config.mode.toUpperCase()) {
      case "EXPORT":
        await this.exportContent(target,tc,steps)
        break
      case "IMPORTUSINGEXPORT":
        await this.importContent(target,tc,steps)
        break
      case "EXPORTROUNDTRIP":
        await this.exportRoundtrip(target,tc,steps);
        break;
      case "DBROUNDTRIP":
        const clone = (this.config.clone && (this.config.clone === true)) 
        await this.databaseRoundtrip(target,tc,steps,clone)
     default:
    }
       
  }
  
  async doOperations(target,tc,operationPath) {
      
    const operations = require(path.resolve(operationPath))
    for (const steps of operations) {
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

 