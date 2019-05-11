"use strict" 
const fs = require('fs');
const path = require('path');
const Transform = require('stream').Transform;

const YadamuTest = require('./yadamuTest.js')

const OracleCompare = require('./oracleCompare.js');
const MsSQLCompare = require('./mssqlCompare.js');
const MySQLCompare = require('./mysqlCompare.js');
const MariadbCompare = require('./mariadbCompare.js');
const PostgresCompare = require('./postgresCompare.js');
const FileCompare = require('./fileCompare.js');

const FileReader = require('../../file/node/fileReader.js');

const CLARINET = 1;
const RDBMS    = 2;

class TestHarness {

  constructor() {
  
    this.yadamu        = new YadamuTest(); 
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
        return this.yadamu.doImport(dbi,file);
      case RDBMS :
        return this.yadamu.doServerImport(dbi,file);
      default:
        return this.yadamu.doImport(dbi,file);
    }
  }

  
  getCompareParameters(source,target,testParameters) {
      
    const compareParameters = Object.assign({}, testParameters)
    
    const dbList = new Array(source,target)
    
    dbList.forEach(function(db) {    
      switch (db) {
        case "oracle18c" :
        case "oracle18" :
        case "oracle12c" :
        case "oracleXE" :          
          compareParameters.EMPTY_STRING_IS_NULL = true;
          compareParameters.SPATIAL_PRECISION = 13;
          break;
        case "postgres" :
          break;
        case "mssql" :
          compareParameters.SPATIAL_PRECISION = 13;
          compareParameters.STRIP_XML_DECLARATION = true;
          break;
        case "mysql" :
          compareParameters.TABLE_MATCHING = 'INSENSITIVE'
          compareParameters.SPATIAL_PRECISION = 13;
          break;
        case "mariadb" :
          compareParameters.TABLE_MATCHING = 'INSENSITIVE'
          compareParameters.SPATIAL_PRECISION = 13;
          break;
        case "file" :
          break;
        default:   
          console.log('Invalid Database: ',db);  
        }
      },this)
      return compareParameters;
  }
  
  async compareSchemas(db,sourceSchema,targetSchema,timings,compareParameters) {

     const dbi = this.getDatabaseInterface(db);
     dbi.configureTest(this.ros,this.connections[db],compareParameters)
     await dbi.initialize();
     await dbi.report(sourceSchema,targetSchema,timings);
     await dbi.finalize();
     
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
  
    
  fileRoundtripResults(operationsList,elapsedTime) {
  
    this.ros.write(`${new Date().toISOString()}[ROUNDTRIP: Operation complete] `);
    this.ros.write(`SOURCE:[${operationsList[0]}] -->  [${operationsList[1]}] --> [${operationsList[2]}] --> [${operationsList[3]}]  --> [${operationsList[4]}]. Elapsed Time: ${elapsedTime}ms.\n`);

  }  

  async fileRoundtrip(db,parameters,sourceFile,targetSchema1,targetFile1,targetSchema2,targetFile2) {
      
      const source = 'file';
      const timings = []
      const testRoot = path.join('work',db);

      const opStartTime = new Date().getTime();
      const operationsList = [sourceFile]

      const dbSchema1 = this.getDatabaseSchema(db,targetSchema1)     
      await this.recreateSchema(db,this.connections[db],dbSchema1);     

      let testParameters = parameters ? Object.assign({},parameters) : {}
      let dbi = this.getTestInterface(db,'TOUSER',dbSchema1,testParameters,this.connections[db]);     
      
      let startTime = new Date().getTime();
      timings[0] = await this.doImport(dbi,sourceFile);
      let elapsedTime = new Date().getTime() - startTime;

      let targetDescription = db === 'mssql' ? `${dbSchema1.schema}"."${dbSchema1.owner}` : dbSchema1  
      operationsList.push(`"${db}"://"${targetDescription}"`)
      this.printResults(`"${source}"://"${sourceFile}"`,`"${db}"://"${targetDescription}"`,elapsedTime)

      testParameters = parameters ? Object.assign({},parameters) : {}
      dbi = this.getTestInterface(db,'OWNER',dbSchema1,testParameters,this.connections[db]);     
      startTime = new Date().getTime()
      timings[1] = await this.yadamu.doExport(dbi,path.join(testRoot,targetFile1));
      elapsedTime = new Date().getTime() - startTime;

      let sourceDescription = targetDescription;
      operationsList.push(`"${source}"://"${targetFile1}"`)
      this.printResults(`"${db}"://"${sourceDescription}"`,`"${source}"://"${targetFile1}"`,elapsedTime)

      testParameters = parameters ? Object.assign({},parameters) : {}
      const dbSchema2 = this.getDatabaseSchema(db,targetSchema2)     
      await this.recreateSchema(db,this.connections[db],dbSchema2);     

      testParameters = parameters ? Object.assign({},parameters) : {}
      dbi = this.getTestInterface(db,'TOUSER',dbSchema2,testParameters,this.connections[db]);     


      startTime = new Date().getTime();
      timings[2] = await this.doImport(dbi,path.join(testRoot,targetFile1));
      elapsedTime = new Date().getTime() - startTime;
      
      targetDescription = db === 'mssql' ? `${dbSchema2.schema}"."${dbSchema2.owner}` : dbSchema2      
      operationsList.push(`"${db}"://"${targetDescription}"`)
      this.printResults(`"${source}"://"${targetFile1}"`,`"${db}"://"${targetDescription}"`,elapsedTime)

      testParameters = parameters ? Object.assign({},parameters) : {}
      dbi = this.getTestInterface(db,'OWNER',dbSchema2,testParameters,this.connections[db]);     

      startTime = new Date().getTime()
      timings[3] = await this.yadamu.doExport(dbi,path.join(testRoot,targetFile2));
      elapsedTime = new Date().getTime() - startTime;
      
      sourceDescription = targetDescription;
      operationsList.push(`"${source}"://"${targetFile2}"`)
      this.printResults(`"${db}"://"${sourceDescription}"`,`"${source}"://"${targetFile2}"`,elapsedTime)
      
      const opElapsedTime =  new Date().getTime() - opStartTime

      await this.compareSchemas(db,dbSchema1,dbSchema2,timings,{});
    
      const fc = new FileCompare(this.yadamu);
      testParameters = dbi.parameters.TABLE_MATCHING ? {TABLE_MATCHING : dbi.parameters.TABLE_MATCHING} : {}
      fc.configureTest(this.ros,{},testParameters)
      await fc.report(sourceFile, path.join(testRoot,targetFile1), path.join(testRoot,targetFile2), timings);    

      this.fileRoundtripResults(operationsList,opElapsedTime)
      
  }
  
  propogateTableMatching(sourceDB,targetDB) {
    if (sourceDB.parameters.TABLE_MATCHING && !targetDB.parameters.TABLE_MATCHING) {
      targetDB.parameters.TABLE_MATCHING = sourceDB.parameters.TABLE_MATCHING
    }
    else {
      if (targetDB.parameters.TABLE_MATCING && !sourceDB.parameters.TABLE_MATCHING) {
        sourceDB.parameters.TABLE_MATCHING = targetDB.parameters.TABLE_MATCHING
      }
    }
    return sourceDB.parameters.TABLE_MATCHING
  }

  
  dbRoundtripResults(operationsList,elapsedTime) {
  
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
      this.ros.write(`${new Date().toISOString()}[ROUNDTRIP: Operation complete] `);
      this.ros.write(`SOURCE:[${operationsList[0]}] --> `);
      if (operationsList.length === 3) {
        this.ros.write(`[${operationsList[1]}] --> `);
      }
      this.ros.write(`TARGET:[${operationsList[operationsList.length-1]}]. Elapsed Time: ${elapsedTime}ms.\n`);
    }
  
  }

  async importResults(db,target,timings) {

     const dbi = this.getDatabaseInterface(db);
     dbi.configureTest(this.ros,this.connections[db],{})
     await dbi.initialize();
     await dbi.importResults(target,timings);
     await dbi.finalize();
     
  }

  async databaseRoundtrip(source,target,clone,parameters,steps) {
       
      let startTime
      let elapsedTime
      let tableMatchingStrategy = undefined;
  
      const timings = []
            
      /*
      **
      ** If source vendor and target vendor are the same test consists of a single operation. The operation does a DDL_AND_DATA mode copy of the data from the source schema to the target schema.
      **
      **
      ** If source vendor and target vendor are difference the test consists of two, or possiblly three operations.
      **
      ** If clone mode = true 
      ** 
      */
            
      /*
      **    Ensure that the table structure of the target is a direct clone of the source.
      **
      **    If source = target then both database are managed by the same vendor. The test consists of a single operation. The operation is a direct clone of the source schema into the target schema. MODE is DDL_AND_DLL. 
      ** 
      **    If source != target then we are copying data from the source vendor to the target vendor and back to the source vendor.
      **
      **       The first operation is a direct clone from the source schema to the target schema. The mode is DDL_ONLY. This clones the source schemas structure in the target schema.
      **       The second operations is a copy from the source vendor to the target vendor. The mode is DDL_AND_DATA.
      **       The third operation is a copy from the target vendor back to the source vendor using the schema created in the first operation. 
      **
      */


      let targetSchema  = undefined;
      let sourceDescription = undefined;

      const operationsList = []
      const dbStartTime = new Date().getTime();

      const originalSchema = this.getDatabaseSchema(source,steps[0])  
      
      let testParameters = parameters ? Object.assign({},parameters) : {}

      if (source === target) {
        // Only one operation
        testParameters.MODE = 'DDL_AND_DATA';
        targetSchema = this.getDatabaseSchema(target,steps[1])  
        const targetDescription = target === 'mssql' ? `${targetSchema.schema}"."${targetSchema.owner}` : targetSchema
        const sourceSchema = originalSchema
        sourceDescription = source === 'mssql' ? `${sourceSchema.schema}"."${sourceSchema.owner}` : sourceSchema
        targetSchema = this.getDatabaseSchema(source,steps[1])  
        await this.recreateSchema(source,this.connections[target],targetSchema);
        const sourceDB = this.getTestInterface(source,'OWNER',sourceSchema,testParameters,this.connections[source]);
        const targetDB = this.getTestInterface(source,'TOUSER',targetSchema,testParameters,this.connections[source]);
        startTime = new Date().getTime();
        timings.push(await this.yadamu.pumpData(sourceDB,targetDB));
        elapsedTime = new Date().getTime() - startTime
        operationsList.push(`"${target}"://"${sourceDescription}"`)
        operationsList.push(`"${source}"://"${targetDescription}"`)
        this.printResults(`"${target}"://"${sourceDescription}"`,`"${source}"://"${targetDescription}"`,elapsedTime)
      }
      else {
        // Two or more operations
        if (clone === true) {
          // Copy Source to Source DDL_ONLY
          testParameters.MODE = 'DDL_ONLY';
          const sourceSchema = originalSchema
          const sourceDescription = source === 'mssql' ? `${sourceSchema.schema}"."${sourceSchema.owner}` : sourceSchema
          const targetSchema = this.getDatabaseSchema(source,steps[2])  
          const targetDescription = source === 'mssql' ? `${targetSchema.schema}"."${targetSchema.owner}` : targetSchema
          await this.recreateSchema(source,this.connections[source],targetSchema);
          const sourceDB = this.getTestInterface(source,'OWNER',sourceSchema,testParameters,this.connections[source]);
          const targetDB = this.getTestInterface(source,'TOUSER',targetSchema,testParameters,this.connections[source]);
          startTime = new Date().getTime();
          timings.push(await this.yadamu.pumpData(sourceDB,targetDB));
          elapsedTime = new Date().getTime() - startTime
          this.printResults(`"${source}"://"${sourceDescription}" [Clone]`,`"${source}"://"${targetDescription}" [Clone]`,elapsedTime)
        }
        // Copy Source to Target
        testParameters = parameters ? Object.assign({},parameters) : {}
        let sourceSchema = originalSchema
        sourceDescription = source === 'mssql' ? `${sourceSchema.schema}"."${sourceSchema.owner}` : sourceSchema
        targetSchema = this.getDatabaseSchema(target,steps[1])  
        await this.recreateSchema(target,this.connections[target],targetSchema);
        let targetDescription = target === 'mssql' ? `${targetSchema.schema}"."${targetSchema.owner}` : targetSchema
        let sourceDB = this.getTestInterface(source,'OWNER',sourceSchema,testParameters,this.connections[source]);
        let targetDB = this.getTestInterface(target,'TOUSER',targetSchema,testParameters,this.connections[target]);
        this.propogateTableMatching(sourceDB,targetDB);
        startTime = new Date().getTime();
        timings.push(await this.yadamu.pumpData(sourceDB,targetDB));
        elapsedTime = new Date().getTime() - startTime
        operationsList.push(`"${source}"://"${sourceDescription}"`)
        this.printResults(`"${source}"://"${sourceDescription}"`,`"${target}"://"${targetDescription}"`,elapsedTime)
        // Copy Target to Source
        sourceSchema = targetSchema
        sourceDescription = target === 'mssql' ? `${sourceSchema.schema}"."${sourceSchema.owner}` : sourceSchema
        targetSchema = this.getDatabaseSchema(source,steps[2])  
        targetDescription = source === 'mssql' ? `${targetSchema.schema}"."${targetSchema.owner}` : targetSchema
        sourceDB = this.getTestInterface(target,'OWNER',sourceSchema,testParameters,this.connections[target]);
        targetDB = this.getTestInterface(source,'TOUSER',targetSchema,testParameters,this.connections[source]);
        this.propogateTableMatching(sourceDB,targetDB);
        startTime = new Date().getTime();
        timings.push(await this.yadamu.pumpData(sourceDB,targetDB));
        elapsedTime = new Date().getTime() - startTime
        operationsList.push(`"${target}"://"${sourceDescription}"`)
        operationsList.push(`"${source}"://"${targetDescription}"`)
        this.printResults(`"${target}"://"${sourceDescription}"`,`"${source}"://"${targetDescription}"`,elapsedTime)
      }        

      const dbElapsedTime =  new Date().getTime() - dbStartTime
         
      await this.compareSchemas(source, originalSchema, targetSchema, timings, this.getCompareParameters(source,target,testParameters))
      this.dbRoundtripResults(operationsList,dbElapsedTime)
      
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

      const testParameters = parameters ? parameters : {}
      
      if (source === 'file') {
        const file = directory ? path.join(directory,sourceInfo) : sourceInfo
        sourceDescription = file;
        sourceDB = new FileReader(this.yadamu);
        sourceDB.configureTest({},{FILE : file},{});
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
      
      if (target === 'file') {
      }
      else {
        await this.importResults(target,dbSchema,timings);
      }

      
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
    const harness = new TestHarness();
    await harness.runTests();
  } catch (e) {
    console.log(e);
  }
}

main();