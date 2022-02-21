"use strict"

import fs from 'fs'
import path from 'path'
import { performance } from 'perf_hooks';

import Yadamu from '../../YADAMU/common/yadamu.js';
import YadamuLogger from '../../YADAMU/common/yadamuLogger.js';
import YadamuLibraray from '../../YADAMU/common/yadamuLibrary.js';
import HttpDBI from './httpDBI.js';
import FileDBI from '../../YADAMU/file/node/fileDBI.js'

class YadamuServer {

  loadJSON(path) {
  
    /*
    ** 
    ** Use instead of 'requiring' configuration files. Avoids loading configuration files into node's "Require" cache
    **
    ** ### TODO : Check file exists and has reasonable upper limit for size before processeing
    ** 
    */ 
		  
    const fileContents = fs.readFileSync(path);
	
	try {
	  return JSON.parse(fileContents);
    } catch (e) {
	  this.yadamuLogger.error([`${this.constructor.name}.loadJSON()`],`JSON processing error while processing "${path}"`)
      throw e
	}
  }

  validateConfiguration() {
  }

  expandConfiguration(configuration) {
      
    if (typeof configuration.connections === "string") {
      configuration.connections = this.loadJSON(path.resolve(configuration.connections))
    }
    if (typeof configuration.schemas === "string") {
      configuration.schemas = this.loadJSON(path.resolve(configuration.schemas))
    }
    this.validateConfiguration()
  }
  
 getDatabaseInterface(yadamu,driver,connectionProperties,parameters) {
    
    let dbi = undefined
    
    switch (driver) {
      case "oracle"  : 
        const OracleDBI = require('../../YADAMU/oracle/node/oracleDBI.js');
        dbi = new OracleDBI(yadamu)
        break;
      case "postgres" :
        const PostgresDBI = require('../../YADAMU/postgres/node/postgresDBI.js');
        dbi = new PostgresDBI(yadamu)
        break;
      case "mssql" :
        const MsSQLDBI = require('../../YADAMU/mssql/node/msSQLDBI.js');
        dbi = new MsSQLDBI(yadamu)
        break;
      case "mysql" :
        const MySQLDBI = require('../../YADAMU/mysql/node/mySQLDBI.js');
        dbi = new MySQLDBI(yadamu)
        break;
      case "mariadb" :
        const MariaDBI = require('../../YADAMU/mariadb/node/mariaDBI.js');
        dbi = new MariaDBI(yadamu)
        break;
      case "mongodb" :
        const MongoDBI = require('../../YADAMU/mongodb/node/mongoDBI.js');
        dbi = new MongoDBI(yadamu)
        break;
      case "snowflake" :
        const SnowflakeDBI = require('../../YADAMU/snowflake/node/snowflakeDBI.js');
        dbi = new SnowflakeDBI(yadamu)
        break;
      case "file" :
        dbi = new FileDBI(yadamu)
        break;
      default:   
        this.yadamuLogger.info([`${this.constructor.name}.getDatabaseInterface()`,`${driver}`],`Unknown Database.`);  
      }
	  
	  dbi.setConnectionProperties(connectionProperties);
      dbi.setParameters(parameters);
      return dbi;
  }
  
  getUser(vendor,schema) {
    
     return vendor === 'mssql' ? schema.owner : (vendor === 'snowflake' ? schema.snowflake.schema : schema.schema)
     
  }
  
  constructor () {
    this.status = {
      operation     : 'SERVER'
     ,errorRaised   : false
     ,warningRaised : false
     ,statusMsg     : 'successfully'
     ,startTime     : performance.now()
    }

    this.yadamuLogger = new YadamuLogger(process.stdout,this.status);
	this.configuration = this.loadJSON(process.argv[2])
	this.expandConfiguration(this.configuration)
  }
	
	
  initialize() {
  }
	
  async exportStream(request,response) {
   
	  const sourceConnection = this.configuration.connections[request.params.connection]
      const sourceSchema = this.configuration.schemas[request.params.schema]
      const sourceDatabase =   YadamuLibrary.getVendorName(sourceConnection)
      const yadamu = new Yadamu('HTTP',{});
	  const sourceParameters = {
		FROM_USER: this.getUser(sourceDatabase,sourceSchema)
	  }
      const sourceDBI = this.getDatabaseInterface(yadamu,sourceDatabase,sourceConnection,sourceParameters);
	  const targetDBI = new HttpDBI(yadamu,response)
	  response.type('json')
	  await yadamu.doCopy(sourceDBI,targetDBI);  
	  response.end();	  
  }

  async importStream(request,response) {
	  
      const targetConnection = this.configuration.connections[request.params.connection]
      const targetSchema = this.configuration.schemas[request.params.schema]
      const targetDatabase = YadamuLibrary.getVendorName(targetConnection);
      const yadamu = new Yadamu('HTTP',{});
	  const targetParameters = {
		TO_USER: this.getUser(targetDatabase,targetSchema)
	  }
      const targetDBI = this.getDatabaseInterface(yadamu,targetDatabase,targetConnection,targetParameters);
	  const sourceDBI = new HttpDBI(yadamu,request)
	  
	  response.type('text')
	  yadamu.getYadamuLogger().switchOutputStream(response);
	  await yadamu.doCopy(sourceDBI,targetDBI); 
	  response.end();	
	  
  }

  async exportFile(request,response) {
	  
	  const sourceConnection = this.configuration.connections[request.params.connection]
      const sourceSchema = this.configuration.schemas[request.params.schema]
      const sourceDatabase =   YadamuLibrary.getVendorName(sourceConnection)
      const yadamu = new Yadamu('HTTP',{});
	  const sourceParameters = {
		FROM_USER: this.getUser(sourceDatabase,sourceSchema)
	  }
      const sourceDBI = this.getDatabaseInterface(yadamu,sourceDatabase,sourceConnection,sourceParameters);

      const targetConnection = this.configuration.connections[request.params.directory]
	  const targetParameters = {
		FILE: path.join(targetConnection.file.directory,request.params.file)
	  }
      const targetDBI = this.getDatabaseInterface(yadamu,'file',{},targetParameters);
	  
	  response.type('text')
	  yadamu.getYadamuLogger().switchOutputStream(response);
	  await yadamu.doCopy(sourceDBI,targetDBI);  
	  response.end();	    
  }


  async importFile(request,response) {  

      const sourceConnection = this.configuration.connections[request.params.directory]
	  const sourceParameters = {
		FILE: path.join(sourceConnection.file.directory,request.params.file)
	  }
      const sourceDBI = this.getDatabaseInterface(yadamu,'file',{},sourceParameters);
	  
	  const targetConnection = this.configuration.connections[request.params.connection]
      const targetSchema = this.configuration.schemas[request.params.schema]
      const targetDatabase =  YadamuLibrary.getVendorName(targetConnection);
      const yadamu = new Yadamu('HTTP',{});
	  const targetParameters = {
		TO_USER: this.getUser(targetDatabase,targetSchema)
	  }
      const targetDBI = this.getDatabaseInterface(yadamu,targetDatabase,targetConnection,targetParameters);

	  response.type('text')
	  yadamu.getYadamuLogger().switchOutputStream(response);
	  await yadamu.doCopy(sourceDBI,targetDBI);  
	  response.end();	  

  }
 

  async copy(request,response) {

      const yadamu = new Yadamu('HTTP',{});

	  const sourceConnection = this.configuration.connections[request.params.connection]
      const sourceSchema = this.configuration.schemas[request.params.schema]
      const sourceDatabase =   YadamuLibrary.getVendorName(sourceConnection)
	  const sourceParameters = {
		FROM_USER: this.getUser(sourceDatabase,sourceSchema)
	  }
      const sourceDBI = this.getDatabaseInterface(yadamu,sourceDatabase,sourceConnection,sourceParameters);

	  const targetConnection = this.configuration.connections[request.params.connection]
      const targetSchema = this.configuration.schemas[request.params.schema]
      const targetDatabase =  YadamuLibrary.getVendorName(targetConnection);
	  const targetParameters = {
		TO_USER: this.getUser(targetDatabase,targetSchema)
	  }
      const targetDBI = this.getDatabaseInterface(yadamu,targetDatabase,targetConnection,targetParameters);

	  response.type('text')
	  yadamu.getYadamuLogger().switchOutputStream(response);
	  await yadamu.doCopy(sourceDBI,targetDBI);  
	  response.end();	  

  }
	
  async uploadConnectionDefinitions(request,response) {
  }
	  
  async uploadSchemaMappings(request,response) {
  }

  async about(request,response) {
	response.type('text')
	response.write('YadamuServer v0.1. Copyright Yet Another Bay Area Software Company 2020.');
	response.end();
  }
	  
}

export { YadamuServer as default }