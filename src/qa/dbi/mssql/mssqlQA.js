
import {
  setTimeout 
}                      from "timers/promises"

import MsSQLDBI        from '../../../node/dbi//mssql/mssqlDBI.js';
import MsSQLError      from '../../../node/dbi//mssql/mssqlException.js'
import MsSQLConstants  from '../../../node/dbi//mssql/mssqlConstants.js';

import Yadamu          from '../../core/yadamu.js';
import YadamuQALibrary from '../../lib/yadamuQALibrary.js'

class MsSQLQA extends YadamuQALibrary.qaMixin(MsSQLDBI) {

    static #_DBI_PARAMETERS
    
    static get DBI_PARAMETERS()  { 
       this.#_DBI_PARAMETERS = this.#_DBI_PARAMETERS || Object.freeze(Object.assign({},Yadamu.DBI_PARAMETERS,MsSQLConstants.DBI_PARAMETERS,Yadamu.QA_CONFIGURATION[MsSQLConstants.DATABASE_KEY] || {},{RDBMS: MsSQLConstants.DATABASE_KEY}))
       return this.#_DBI_PARAMETERS
    }
   
    get DBI_PARAMETERS() {
      return MsSQLQA.DBI_PARAMETERS
    }   
        
    constructor(yadamu,manager,connectionSettings,parameters) {
       super(yadamu,manager,connectionSettings,parameters)
    }
     
    async initialize() {
                
      // Must (re) create the database before attempting to connection. initialize() will fail if the database does not exist.
      if (this.options.recreateSchema === true) {
        await this.recreateDatabase();
        this.options.recreateSchema = false
      }
      await super.initialize();
      
    }      
    
    /*
    **
    ** The "Recreate Schema" option is problematic with SQL Server. 
    ** In SQL Server testing Schemas are mapped to databases, since there is no simple mechanism for dropping a schema cleanly in SQL Server.
    ** This means we have to deal with two scenarios when recreating a schema. First the required database may not exist, second it exists and needs to be dropped and recreated.
    ** Connect attempts fail if the target database does exist. This means that it necessary to connect to a known good database while the target database is recreated.
    ** After creating the database the connection must be closed and a new connection opened to the target database.
    **
    */    

    async recreateDatabase() {

      try { 
        const vendorProperties = Object.assign({},this.vendorProperties)
        const dbi = new MsSQLDBMgr(this.yadamu, vendorProperties)
        await dbi.recreateDatabase(this.parameters.YADAMU_DATABASE)
      } catch (e) {
        this.yadamu.LOGGER.handleException([this.DATABASE_VENDOR,'RECREATE DATABASE',this.parameters.YADAMU_DATABASE],e);
        throw e
      }
    }

    classFactory(yadamu) {
      return new MsSQLQA(yadamu,this,this.connectionParameters,this.parameters)
    }
	
	/*
       
    async bulkInsert(bulkOperation) {
	  
	  this.batchNumber = this.batchNumber ? this.batchNumber + 1  : 1
	  if (this.batchNumber === 5) {
	    this.yadamu.killConfiguration = {delay: 1100}
	    const pid = await this.getConnectionID();
        await this.manager.scheduleTermination(pid,this.getWorkerNumber())
	  }
	  await super.bulkInsert(bulkOperation)
	}
	  

    */
	
    async scheduleTermination(pid,workerId) {
      const tags = this.getTerminationTags(workerId,pid)
      this.LOGGER.qa(tags,`Termination Scheduled.`);
      setTimeout(this.yadamu.KILL_DELAY,pid,{ref: false}).then(async (pid) => {
        if (this.pool !== undefined) {
          this.LOGGER.log(tags,`Killing connection.`);
          // Do not use getRequest() as it will fail with "There is a request in progress during write opeations. Get a non pooled request
          // const request = new this.sql.Request(this.pool);
          const request = await this.sql.connect(this.vendorProperties);
          let stack
          const sqlStatement = `kill ${pid}`
          try {
            stack = new Error().stack
            const res = await request.query(sqlStatement);
          } catch (e) {
            if (e.number && (e.number === 6104)) {
              // Msg 6104, Level 16, State 1, Line 1 Cannot use KILL to kill your own process
              this.LOGGER.log(tags,`Worker finished prior to termination.`)
            }
            else if (e.number && (e.number === 6106)) {
              // Msg 6106, Level 16, State 2, Line 1 Process ID 54 is not an active process ID.
              this.LOGGER.log(tags,`Worker finished prior to termination.`)
            }
            else {
              const cause = this.createDatabaseError(this.DRIVER_ID,e,stack,sqlStatement)
              this.LOGGER.handleException(tags,cause)
            }
          } 
        }
        else {
          this.LOGGER.log(tags,`Unable to Kill Connection: Connection Pool no longer available.`);
        }
	  })
    }

    
}

class MsSQLDBMgr extends MsSQLQA {
    
    constructor(yadamu,vendorProperties) {
      super(yadamu)
      this.vendorProperties = vendorProperties
      this.vendorProperties.database = 'master';
    }
    
    async initialize() {
      await this._getDatabaseConnection()
    }
  
    async recreateDatabase(database) {

       const SINGLE_USER_MODE = `if DB_ID('${database}') IS NOT NULL alter database [${database}] set single_user with rollback immediate` 
       const DROP_DATABASE = `if DB_ID('${database}') IS NOT NULL drop database [${database}]`
  
    
      try {
        await this.initialize()
        // Create a connection pool using a well known database that must exist   
        this.vendorProperties.database = 'master';
        // await super.initialize();

        let results;       
        
        results =  await this.executeSQL(SINGLE_USER_MODE);      
        results =  await this.executeSQL(DROP_DATABASE);      
        const CREATE_DATABASE = `create database "${database}" COLLATE ${this.DB_COLLATION}`;
        results =  await this.executeSQL(CREATE_DATABASE);      
		
		await this.final()

      } catch (e) {
        // console.log([this.DATABASE_VENDOR,'recreateDatabase()'],e);
		await this.destroy(e)
        throw e
      }
      
    }   
}  

export { MsSQLQA as default }