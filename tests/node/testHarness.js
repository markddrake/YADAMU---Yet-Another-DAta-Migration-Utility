"use strict" 
const fs = require('fs');
const fsPromises = require('fs').promises;
const path = require('path');
const Transform = require('stream').Transform;

const YadamuTest = require('./yadamuTest.js')

const YadamuLogger = require('../../common/yadamuLogger.js');
const FileReader = require('../../file/node/fileReader.js');

const OracleCompare = require('./oracleCompare.js');
const MsSQLCompare = require('./mssqlCompare.js');
const MySQLCompare = require('./mysqlCompare.js');
const MariadbCompare = require('./mariadbCompare.js');
const PostgresCompare = require('./postgresCompare.js');
const MongoCompare = require('./mongoCompare.js');
const FileCompare = require('./fileCompare.js');


const CLARINET = 1;
const RDBMS    = 2;

class TestHarness {

  constructor() {
  
    this.yadamu            = new YadamuTest();     
    this.testConfiguration = require(path.resolve(this.yadamu.getParameters().CONFIG))
    this.connections       = require(path.resolve(this.testConfiguration.connections))

    // Expand environemnt variables in path using regex.
    this.yadamuLogger = this.testConfiguration.outputFile ? new YadamuLogger(fs.createWriteStream(path.resolve(this.testConfiguration.outputFile.replace(/%([^%]+)%/g, (_,n) => process.env[n]))),{}) : this.yadamu.getYadamuLogger();
  }
  
  getDescription(db,schemaInfo) {
    return db === 'mssql' ? `${schemaInfo.database}"."${schemaInfo.owner}` : schemaInfo.schema
  }
  
  getDatabaseInterface(db) {
  
    let dbi = undefined
    switch (db) {
      case "oracle19c" :
      case "oracle18c" :
      case "oracle12c" :
      case "oracle11g" :
      case "oracleXE"  : 
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
      case "mongodb" :
        dbi = new MongoCompare(this.yadamu)
        break;
      case "file" :
        dbi = new FileCompare(this.yadamu)
        break;
      default:   
        this.yadamuLogger.log([`${this.constructor.name}.getDatabaseInterface()`,`${db}`],`Unknown Database.`);  
      }      
      return dbi;
  }
  
  getTestInterface(db,role,connectInfo,testParameters,testConnection,tableMappings) {

    const parameters = testParameters ? Object.assign({},testParameters) : {}
    const connection = Object.assign({},testConnection)

    this.yadamu.reset();
    const dbi = this.getDatabaseInterface(db)
    
    parameters[role] = (connectInfo.schema ? connectInfo.schema : connectInfo.owner)
    dbi.configureTest(connection,parameters,connectInfo,tableMappings)
    return dbi;    
  }
  
  getDatabaseSchema(db,connectionInfo) {
      
    switch (db) {
      case "mssql":
        if (connectionInfo.schema) {
          return {database : connectionInfo.schema, owner : "dbo" }
        }
        break;
      default:
        if (connectionInfo.database) {
          return {schema : (connectionInfo.owner === 'dbo' ? connectionInfo.database : (connectionInfo.dbPrefix ? connectionInfo.dbPrefix + "_" + connectionInfo.owner : connectionInfo.owner ))}
        }
    }
    return connectionInfo;
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
        case "oracle19c" :
        case "oracle18c" :
        case "oracle12c" :
        case "oracle11g" :
        case "oracleXE"  : 
          compareParameters.EMPTY_STRING_IS_NULL = true;
          compareParameters.SPATIAL_PRECISION = 13;
          compareParameters.MAX_TIMESTAMP_PRECISION = 9;
          break;
        case "postgres" :
          compareParameters.MAX_TIMESTAMP_PRECISION = 6;
          break;
        case "mssql" :
          compareParameters.SPATIAL_PRECISION = 13;
          compareParameters.STRIP_XML_DECLARATION = true;
          compareParameters.MAX_TIMESTAMP_PRECISION = 9;
          break;
        case "mysql" :
          compareParameters.TABLE_MATCHING = 'INSENSITIVE'
          compareParameters.SPATIAL_PRECISION = 13;
          compareParameters.MAX_TIMESTAMP_PRECISION = 6;
          break;
        case "mariadb" :
          compareParameters.TABLE_MATCHING = 'INSENSITIVE'
          compareParameters.SPATIAL_PRECISION = 13;
          compareParameters.MAX_TIMESTAMP_PRECISION = 6;
          break;
        case "mongodb" :
          break;
        case "file" :
          break;
        default:   
          this.yadamuLogger.log([`${this.constructor.name}.getCompareParameters()`,`${db}`],`Unknown Database.`);  
        }
      },this)
      return compareParameters;
  }
  
  checkTimingsForErrors(timings) {
    // If operations failed timings may be undefined. If so replace with empty object to prevent errors when reporting
    timings.forEach(function (t,i) {
      if ((t === undefined) || (t === null)) {
        timings[i] = {}
      }
    },this)
  }
  
  async compareSchemas(db,sourceSchema,targetSchema,timings,compareParameters) {

    const dbi = this.getDatabaseInterface(db);
    dbi.configureTest(this.connections[db],compareParameters)
    await dbi.initialize();
    const report = await dbi.report(sourceSchema,targetSchema,timings);
    await dbi.finalize();
     
    const colSizes = [12, 32, 32, 48, 14, 14, 14, 14, 72]
    
    let seperatorSize = (colSizes.slice(0,7).length *3) - 1
    colSizes.slice(0,7).forEach(function(size) {
      seperatorSize += size;
    },this);
    
    
    report.successful.sort().forEach(function(row,idx) {
      if (idx === 0) {
        this.yadamuLogger.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n') 
        this.yadamuLogger.writeDirect(`|`
                                    + ` ${'RESULT'.padEnd(colSizes[0])} |`
                                    + ` ${'SOURCE SCHEMA'.padStart(colSizes[1])} |`
                                    + ` ${'TARGET SCHEMA'.padStart(colSizes[2])} |` 
                                    + ` ${'TABLE_NAME'.padStart(colSizes[3])} |`
                                    + ` ${'ROWS'.padStart(colSizes[4])} |`
                                    + ` ${'ELAPSED TIME'.padStart(colSizes[5])} |`
                                    + ` ${'THROUGHPUT'.padStart(colSizes[6])} |`
                           + '\n');
        this.yadamuLogger.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n') 
        this.yadamuLogger.writeDirect(`|`
                                    + ` ${'SUCCESSFUL'.padEnd(colSizes[0])} |`
                                    + ` ${row[0].padStart(colSizes[1])} |`
                                    + ` ${row[1].padStart(colSizes[2])} |`)
      }
      else {
        this.yadamuLogger.writeDirect(`|`
                                    + ` ${''.padEnd(colSizes[0])} |`
                                    + ` ${''.padStart(colSizes[1])} |`
                                    + ` ${''.padStart(colSizes[2])} |` )
      }

      this.yadamuLogger.writeDirect(` ${row[2].padStart(colSizes[3])} |` 
                                  + ` ${row[3].toString().padStart(colSizes[4])} |` 
                                  + ` ${YadamuTest.stringifyDuration(parseInt(row[4])).padStart(colSizes[5])} |` 
                                  + ` ${(row[5] === 'NaN/s' ? '' : row[5]).padStart(colSizes[6])} |` 
                         + '\n');
    },this)
        
    if (report.successful.length > 0) {
      this.yadamuLogger.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n\n') 
    }
      
    seperatorSize = (colSizes.length * 3) - 1;
    colSizes.forEach(function(size) {
      seperatorSize += size;
    },this);
   
    const notesIdx = colSizes.length-2
    const lineSize = colSizes[notesIdx+1]
   
    report.failed.forEach(function(row,idx) {

      const lines = []
      if ((row[notesIdx] !== null) && ((row[notesIdx].length > lineSize) || (row[notesIdx].indexOf('\r\n') > -1))) {
        const blocks = row[7].split('\r\n')
        for (const block of blocks) {
          const words = block.split(' ')
          let line = ''
          for (let word of words) {
            if (line.length > 0) {
              word = ' ' + word
            }
            if (line.length + word.length < lineSize) {
              line = line + word
            }
            else {
              if (line.length > 0) {
                // Push Line and start new line
                lines.push(line)
                word = word.substring(1)
              }
              while (word.length > lineSize) {
                if (word[lineSize-1] === '-') {
                  lines.push(word.substring(0,lineSize))
                  word = word.substring(lineSize)
                }  
                else {
                  lines.push(word.substring(0,lineSize-1) + "-")
                  word = word.substring(lineSize-1)
                }
              }
              line = word
            }
          }
          if (line.length > 0) {
             lines.push(line)
          }
        }
        row[7] = lines.shift()
      } 
      if (idx === 0) {
        this.yadamuLogger.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n') 
        this.yadamuLogger.writeDirect(`|`
                                    + ` ${'RESULT'.padEnd(colSizes[0])} |`
                                    + ` ${'SOURCE SCHEMA'.padStart(colSizes[1])} |`
                                    + ` ${'TARGET SCHEMA'.padStart(colSizes[2])} |` 
                                    + ` ${'TABLE_NAME'.padStart(colSizes[3])} |`
                                    + ` ${'SOURCE ROWS'.padStart(colSizes[4])} |`
                                    + ` ${'TARGET ROWS'.padStart(colSizes[5])} |`
                                    + ` ${'MISSING ROWS'.padStart(colSizes[6])} |`
                                    + ` ${'EXTRA ROWS'.padStart(colSizes[7])} |`
                                    + ` ${'NOTES'.padEnd(colSizes[8])} |`
                           + '\n');
        this.yadamuLogger.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n') 
        this.yadamuLogger.writeDirect(`|`
                                    + ` ${'FAILED'.padEnd(colSizes[0])} |`
                                    + ` ${row[0].padStart(colSizes[1])} |`
                                    + ` ${row[1].padStart(colSizes[2])} |`) 
      }
      else {
        this.yadamuLogger.writeDirect(`|`
                                    + ` ${''.padEnd(colSizes[0])} |`
                                    + ` ${''.padStart(colSizes[1])} |`
                                    + ` ${''.padStart(colSizes[2])} |`)
      }
                
      this.yadamuLogger.writeDirect(` ${row[2].padStart(colSizes[3])} |` 
                                  + ` ${row[3].toString().padStart(colSizes[4])} |` 
                                  + ` ${row[4].toString().padStart(colSizes[5])} |` 
                                  + ` ${row[5].toString().padStart(colSizes[6])} |` 
                                  + ` ${row[6].toString().padStart(colSizes[7])} |` 
                                  + ` ${(row[7] !== null ? row[7] :  '').padEnd(colSizes[8])} |` 
                         + '\n');

                               
      lines.forEach(function(line) {
        this.yadamuLogger.writeDirect(`|`
                                    + ` ${''.padEnd(colSizes[0])} |`
                                    + ` ${''.padStart(colSizes[1])} |`
                                    + ` ${''.padStart(colSizes[2])} |`
                                    + ` ${''.padStart(colSizes[3])} |` 
                                    + ` ${''.padStart(colSizes[4])} |` 
                                    + ` ${''.padStart(colSizes[5])} |` 
                                    + ` ${''.padStart(colSizes[6])} |` 
                                    + ` ${''.padStart(colSizes[7])} |` 
                                    + ` ${line.padEnd(colSizes[8])} |`
                                    + '\n');
      },this)          

    },this)
      
    if (report.failed.length > 0) {
      this.yadamuLogger.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n\n') 
    }
  }
  
  printResults(sourceDescription,targetDescription,elapsedTime) {
  
    if (!this.yadamuLogger.loggingToConsole()) {
      
      const colSizes = [24,128,12]
      let seperatorSize = (colSizes.length * 3) - 1;
      colSizes.forEach(function(size) {
        seperatorSize += size;
      },this);
    
      this.yadamuLogger.writeDirect('\n+' + '-'.repeat(seperatorSize) + '+' + '\n') 
     
      this.yadamuLogger.writeDirect(`| ${'TIMESTAMP'.padEnd(colSizes[0])} |`
                                  + ` ${'OPERATION'.padEnd(colSizes[1])} |`
                                  + ` ${'ELASPED TIME'.padStart(colSizes[2])} |` 
                 
                 + '\n');
      this.yadamuLogger.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n') 
      
      this.yadamuLogger.writeDirect(`| ${new Date().toISOString().padEnd(colSizes[0])} |`
                                  + ` ${(sourceDescription + ' --> ' + targetDescription).padEnd(colSizes[1])} |`
                                  + ` ${(elapsedTime.toString()+"ms").padStart(colSizes[2])} |` 
                 + '\n');
                 
      this.yadamuLogger.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n\n') 
    }
    else {
      this.yadamuLogger.log([`${this.constructor.name}`,'COPY'],`Operation complete. Source:[${sourceDescription}]. Target:[${targetDescription}]. Elapsed Time: ${YadamuTest.stringifyDuration(elapsedTime)}s.`);
    }
  
  }
  
  async fileRoundtrip(db,parameters,sourceFile,targetSchema1,targetFile1,targetSchema2,targetFile2) {
      
      const source = 'file';
      const timings = []
      const testRoot = path.join('work',db);

      const opStartTime = new Date().getTime();
      const operationsList = [sourceFile]
      
      let dbSchema1 = targetSchema1
      let dbSchema2 = targetSchema2
      
      if (db === 'mssql') {
		if (dbSchema1.schema) {
        // Map non MsSQL Connection Information to a MsSQL database
	      dbSchema1 = {database : dbSchema1.schema, owner : 'dbo'}
	      dbSchema2 = {database : dbSchema2.schema, owner : 'dbo'}
		} 
      }
	  else {
        if (dbSchema1.database) {
          // Map MsSQL Connection Information to a non MsSQL database
          if (targetSchema1.owner = targetSchema2.owner) {
            dbSchema1 = { schema : dbSchema1.database }
            dbSchema2 = { schema : dbSchema2.database }
          }
          else {
            dbSchema1 = { schema : dbSchema1.owner }
            dbSchema2 = { schema : dbSchema2.owner }
          }
        }
      }
      
      await this.recreateSchema(db,this.connections[db],dbSchema1);     

      let testParameters = parameters ? Object.assign({},parameters) : {}
      let dbi = this.getTestInterface(db,'TOUSER',dbSchema1,testParameters,this.connections[db]);     
      
      let startTime = new Date().getTime();
      timings[0] = await this.doImport(dbi,sourceFile);
      let elapsedTime = new Date().getTime() - startTime;

      let targetDescription = this.getDescription(db,dbSchema1)
      operationsList.push(`"${db}"://"${targetDescription}"`)
      this.printResults(`"${source}"://"${sourceFile}"`,`"${db}"://"${targetDescription}"`,elapsedTime)

      testParameters = parameters ? Object.assign({},parameters) : {}
      dbi = this.getTestInterface(db,'OWNER',dbSchema1,testParameters,this.connections[db],undefined);     
      startTime = new Date().getTime()
      timings[1] = await this.yadamu.doExport(dbi,path.join(testRoot,targetFile1));
      elapsedTime = new Date().getTime() - startTime;
      const tableMappings = dbi.reverseTableMappings();

      let sourceDescription = targetDescription;
      operationsList.push(`"${source}"://"${targetFile1}"`)
      this.printResults(`"${db}"://"${sourceDescription}"`,`"${source}"://"${targetFile1}"`,elapsedTime)

      testParameters = parameters ? Object.assign({},parameters) : {}
      await this.recreateSchema(db,this.connections[db],dbSchema2);     

      testParameters = parameters ? Object.assign({},parameters) : {}
      dbi = this.getTestInterface(db,'TOUSER',dbSchema2,testParameters,this.connections[db],tableMappings);     
      
      startTime = new Date().getTime();
      timings[2] = await this.doImport(dbi,path.join(testRoot,targetFile1));
      elapsedTime = new Date().getTime() - startTime;
      
      targetDescription = this.getDescription(db,dbSchema2)
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
     
      this.checkTimingsForErrors(timings);
      await this.compareSchemas(db,dbSchema1,dbSchema2,timings,{});
    
      const fc = new FileCompare(this.yadamu);      
      testParameters = dbi.parameters.TABLE_MATCHING ? {TABLE_MATCHING : dbi.parameters.TABLE_MATCHING} : {}
      Object.assign(testParameters,parameters);
      fc.configureTest({},testParameters)
      await fc.report(this.yadamuLoggger,sourceFile, path.join(testRoot,targetFile1), path.join(testRoot,targetFile2), timings);    

      this.yadamuLogger.log([`${this.constructor.name}`,'FILECOPY'],`Operation complete: [${operationsList[0]}] -->  [${operationsList[1]}] --> [${operationsList[2]}] --> [${operationsList[3]}]  --> [${operationsList[4]}]. Elapsed Time: ${YadamuTest.stringifyDuration(opEelapsedTime)}s.`);

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
  
    if (!this.yadamuLogger.loggingToConsole()) {
      
      const colSizes = [24,128,12]
      let seperatorSize = (colSizes.length * 3) - 1;
      colSizes.forEach(function(size) {
        seperatorSize += size;
      },this);
    
      this.yadamuLogger.writeDirect('\n+' + '-'.repeat(seperatorSize) + '+' + '\n') 
     
      this.yadamuLogger.writeDirect(`| ${'TIMESTAMP'.padEnd(colSizes[0])} |`
                                  + ` ${'OPERATION'.padEnd(colSizes[1])} |`
                                  + ` ${'ELASPED TIME'.padStart(colSizes[2])} |`     
                         + '\n');
      this.yadamuLogger.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n') 
      
      this.yadamuLogger.writeDirect(`| ${new Date().toISOString().padEnd(colSizes[0])} |`
                                  + ` ${(operationsList[0] + ' --> ' + operationsList[operationsList.length-1]).padEnd(colSizes[1])} |`
                                  + ` ${(elapsedTime.toString()+"ms").padStart(colSizes[2])} |` 
                         + '\n');
                 
      this.yadamuLogger.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n\n') 
    }
    else {
      this.yadamuLogger.log([`${this.constructor.name}`,'DBCOPY'],`Operation complete: Source:[${operationsList[0]}] -->  ${(operationsList.length === 3 ? '[' + operationsList[1] + '] --> ' : '')}Target:${operationsList[operationsList.length-1]}]. Elapsed Time: ${YadamuTest.stringifyDuration(elapsedTime)}s.`);
    }
  
  }

  async importResults(db,target,timings) {

    const dbi = this.getDatabaseInterface(db);
    dbi.configureTest(this.connections[db],{})
     
    await dbi.initialize();
    const report =   await dbi.importResults(target,timings);
    await dbi.finalize();

    const colSizes = [32, 48, 14, 14, 14]
      
    let seperatorSize = (colSizes.length * 3) - 1;
    colSizes.forEach(function(size) {
      seperatorSize += size;
    },this);
   
    report.sort().forEach(function(row,idx) {          
      if (idx === 0) {
        this.yadamuLogger.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n') 
        this.yadamuLogger.writeDirect(`|`
                                    + ` ${'TARGET SCHEMA'.padStart(colSizes[0])} |` 
                                    + ` ${'TABLE_NAME'.padStart(colSizes[1])} |`
                                    + ` ${'ROWS'.padStart(colSizes[2])} |`
                                    + ` ${'ROWS IMPORTED'.padStart(colSizes[3])} |`
                                    + ` ${'DELTA'.padStart(colSizes[4])} |`
                           + '\n');
        this.yadamuLogger.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n') 
        this.yadamuLogger.writeDirect(`|`
                                    + ` ${row[0].padStart(colSizes[0])} |`
                                    + ` ${row[1].padStart(colSizes[1])} |`
                                    + ` ${row[2].toString().padStart(colSizes[2])} |` 
                                    + ` ${row[3].toString().padStart(colSizes[3])} |` 
                                    + ` ${(row[3] - row[2]).toString().padStart(colSizes[4])} |`
                           + '\n');
      }
      else {
        this.yadamuLogger.writeDirect(`|`
                                    + ` ${''.padStart(colSizes[0])} |`
                                    + ` ${row[1].padStart(colSizes[1])} |`
                                    + ` ${row[2].toString().padStart(colSizes[2])} |` 
                                    + ` ${row[3].toString().padStart(colSizes[3])} |` 
                                    + ` ${(row[3] - row[2]).toString().padStart(colSizes[4])} |`
                           + '\n');         
      }
    },this)

    if (report.length > 0) {
      this.yadamuLogger.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n\n') 
    }     
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
        const targetDescription = this.getDescription(target,targetSchema)
        const sourceSchema = originalSchema
        sourceDescription = this.getDescription(source,sourceSchema)
        targetSchema = this.getDatabaseSchema(source,steps[1])  
        await this.recreateSchema(source,this.connections[target],targetSchema);
        const sourceDB = this.getTestInterface(source,'OWNER',sourceSchema,testParameters,this.connections[source],undefined);
        const targetDB = this.getTestInterface(source,'TOUSER',targetSchema,testParameters,this.connections[source],undefined);
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
          const sourceDescription = this.getDescription(source,sourceSchema)
          const targetSchema = this.getDatabaseSchema(source,steps[2])  
          const targetDescription = this.getDescription(source,targetSchema)
          await this.recreateSchema(source,this.connections[source],targetSchema);
          const sourceDB = this.getTestInterface(source,'OWNER',sourceSchema,testParameters,this.connections[source],undefined);
          const targetDB = this.getTestInterface(source,'TOUSER',targetSchema,testParameters,this.connections[source],undefined);
          startTime = new Date().getTime();
          timings.push(await this.yadamu.pumpData(sourceDB,targetDB));
          elapsedTime = new Date().getTime() - startTime
          this.printResults(`"${source}"://"${sourceDescription}" [Clone]`,`"${source}"://"${targetDescription}" [Clone]`,elapsedTime)
        }
        // Copy Source to Target
        testParameters = parameters ? Object.assign({},parameters) : {}
        let sourceSchema = originalSchema
        sourceDescription = this.getDescription(source,sourceSchema)
        targetSchema = this.getDatabaseSchema(target,steps[1])  
        await this.recreateSchema(target,this.connections[target],targetSchema);
        let targetDescription = this.getDescription(target,targetSchema)
        let sourceDB = this.getTestInterface(source,'OWNER',sourceSchema,testParameters,this.connections[source],undefined);
        let targetDB = this.getTestInterface(target,'TOUSER',targetSchema,testParameters,this.connections[target],undefined);
        this.propogateTableMatching(sourceDB,targetDB);
        startTime = new Date().getTime();
        timings.push(await this.yadamu.pumpData(sourceDB,targetDB));
        elapsedTime = new Date().getTime() - startTime
        const tableMappings = targetDB.reverseTableMappings();
        operationsList.push(`"${source}"://"${sourceDescription}"`)
        this.printResults(`"${source}"://"${sourceDescription}"`,`"${target}"://"${targetDescription}"`,elapsedTime)
        // Copy Target to Source
        sourceSchema = targetSchema
        sourceDescription = this.getDescription(target,sourceSchema) 
        targetSchema = this.getDatabaseSchema(source,steps[2])  
        targetDescription =  this.getDescription(source,targetSchema) 
        sourceDB = this.getTestInterface(target,'OWNER',sourceSchema,testParameters,this.connections[target],undefined);
        targetDB = this.getTestInterface(source,'TOUSER',targetSchema,testParameters,this.connections[source],tableMappings);
        this.propogateTableMatching(sourceDB,targetDB);
        startTime = new Date().getTime();
        timings.push(await this.yadamu.pumpData(sourceDB,targetDB));
        elapsedTime = new Date().getTime() - startTime
        operationsList.push(`"${target}"://"${sourceDescription}"`)
        operationsList.push(`"${source}"://"${targetDescription}"`)
        this.printResults(`"${target}"://"${sourceDescription}"`,`"${source}"://"${targetDescription}"`,elapsedTime)
      }        

      const dbElapsedTime =  new Date().getTime() - dbStartTime
      
      this.checkTimingsForErrors(timings);   
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
        sourceDescription = this.getDescription(source,dbSchema)
        sourceDB = this.getTestInterface(source,'OWNER',dbSchema,testParameters,this.connections[source]);
      }

      const dbSchema = this.getDatabaseSchema(target,targetInfo)

      if (target === 'file') {
        const file = directory ? path.join(directory,targetInfo) : targetInfo
        targetDescription = file;
        testParameters.FILE = file
      }
      else {
        targetDescription = this.getDescription(target,dbSchema)
      }
      
      targetDB = this.getTestInterface(target,'TOUSER',dbSchema,testParameters,this.connections[target]);
      
      startTime = new Date().getTime()
      timings[0] = await this.yadamu.pumpData(sourceDB,targetDB);
      elapsedTime = new Date().getTime() - startTime;
      this.printResults(`"${source}"://"${sourceDescription}"`,`"${target}"://"${targetDescription}"`,elapsedTime)
      
      if ((target !== 'file') && (parameters.MODE !== 'DDL_ONLY')) {
        await this.importResults(target,dbSchema,timings);
      }
      
      return timings;

  }
  
  async doCopy(source,target,parameters,directory,steps) {
    await this.copyContent(source,target,parameters,directory,steps[0],steps[1])
    if (steps.length > 2) {
      await this.recreateSchema(source,this.connections[source],steps[2])
      const timings = await this.copyContent(target,source,parameters,directory,steps[1],steps[2])
      if (parameters.MODE !== 'DDL_ONLY') {
        await this.compareSchemas(source,steps[0],steps[2],timings,{});
      }
    }
  }
  
  async doOperation(jobConfiguration,target,steps) {
      
    switch (this.testConfiguration.mode.toUpperCase()) {
      case "EXPORT":
      case "IMPORT":
        await this.doCopy(jobConfiguration.source,target,jobConfiguration.parameters,jobConfiguration.directory,steps)
        break
      case "FILEROUNDTRIP":
        await this.fileRoundtrip(target,jobConfiguration.parameters,steps[0],steps[1],steps[2],steps[3],steps[4]);
        // await this.fileRoundtrip(target,jobConfiguration,steps);
        break;
      case "DBROUNDTRIP":
        const clone = (this.testConfiguration.clone && (this.testConfiguration.clone === true)) 
        await this.databaseRoundtrip(jobConfiguration.source,target,clone,jobConfiguration.parameters,steps)
     default:
    }
       
  }
  
  async runTask(jobConfiguration,target,taskConfigurationPath) {
      
    const operations = JSON.parse(await fsPromises.readFile(path.resolve(taskConfigurationPath)))
    for (const steps of operations) {
      if (jobConfiguration.reverseDirection) {
        if (steps.length % 2 > 0) {
          // ### If Reversing with an odd number of steps remove the last step since it is a verification step.
          steps.pop();
        }
        steps.reverse()
      }
      await this.doOperation(jobConfiguration,target,steps)
    }
  }
  
    async runJobs() {
 
    /*
    **
    ** A test configuration defines test. A test consists of one or more jobs
    **
    ** A test configuration file contains the following information
    **
    **   connections: The path to the file containing the connection information for the databases involved
    **
    **   mode: one of export/import/fileRoundtrip/dbRoundtrip
    **  
    **   clone: true/false  - If true then a DDL_ONLY copy will be used to clone the target schema from the source schema 
    **
    **   outputFile: path to file used to write log information, defautls to standard out.
    **
    **   jobs: The set of job configuration files to be processed as part of the test.
    **
    **  A job configuration file deffines one or more jobs. Each Job specifices
    **
    **    source: The locaton of the source data
    **
    **    targets: The target for the copy operation. There can be more than one target, in which case the tasks will be repeated for each target. 
    **
    **    parser: CLARINET/RDBMS. RDBMS uses the databases json parsing capabilities, such as JSON_TABLE() to process a JSON export file.
    ** 
    **    parameters: The parameters to be supplied to each task.
    **
    **    tasks: A set of tasks (copy operations) to be performed. 
    **
    */
 
    const startTime = new Date().getTime();
    for (const jobConfigurationPath of this.testConfiguration.jobs) {
      const jobConfiguration = require(path.resolve(jobConfigurationPath));
      const startTime = new Date().getTime();
      for (const job of jobConfiguration) {
        if (job.parsingMethod) {
          switch (job.parsingMethod) {
            case "CLARINET" :
              this.parsingMethod = CLARINET;
              break;
            case "RDBMS" :
              this.parsingMethod = RDBMS;
              break;
            default:
              this.parsingMethod = CLARINET;
          }
        }
        const startTime = new Date().getTime();
        for (const target of job.targets) {
          const startTime = new Date().getTime();
          for (const taskConfigurationPath of job.tasks) {
            const startTime = new Date().getTime();
            await this.runTask(job,target,taskConfigurationPath)
            const elapsedTime = new Date().getTime() - startTime;
            this.yadamuLogger.log([`${this.constructor.name}`,'TASK'],`Operation complete: Source:"${job.source}". Target:"${target}". Task:"${taskConfigurationPath}". Elapsed Time: ${YadamuTest.stringifyDuration(elapsedTime)}s.`);
          }
          const elapsedTime = new Date().getTime() - startTime;
          this.yadamuLogger.log([`${this.constructor.name}`,'TARGET'],`Operation complete: Source:"${job.source}". Target:"${target}". Elapsed Time: ${YadamuTest.stringifyDuration(elapsedTime)}s.`);
        }
        const elapsedTime = new Date().getTime() - startTime;
        this.yadamuLogger.log([`${this.constructor.name}`,'JOB'],`Operation complete: Source:"${job.source}". Elapsed Time: ${YadamuTest.stringifyDuration(elapsedTime)}ms.`);
      }
      const elapsedTime = new Date().getTime() - startTime;
      this.yadamuLogger.log([`${this.constructor.name}`,'JOBS'],`Operation complete: Configuration:"${jobConfigurationPath}".  Elapsed Time: ${YadamuTest.stringifyDuration(elapsedTime)}s.`);
    }
    const elapsedTime = new Date().getTime() - startTime;
    this.yadamuLogger.log([`${this.constructor.name}`,'TEST'],`Operation complete: Configuration:"${this.yadamu.getParameters().CONFIG}". Elapsed Time: ${YadamuTest.stringifyDuration(elapsedTime)}s.`);
  
    this.yadamuLogger.close(); 
    this.yadamu.close()
  }
}  

function exit() {
    
  console.log(`[ERROR][TestHarness.exit()]: Forced exit.`);
  process.exit();
  
}

async function main() {
    
  try {
    const harness = new TestHarness();
    await harness.runJobs();
  } catch (e) {
    console.log(`[ERROR][TestHarness.main()]: Unexpected Terminal Exception`);
    console.log(`${(e.stack ? e.stack : e)}`)
    console.log(`[ERROR][TestHarness.main()]: Operation failed.`);
    // setTimeout(exit,1000);
    process.exit();
  }
}

main();