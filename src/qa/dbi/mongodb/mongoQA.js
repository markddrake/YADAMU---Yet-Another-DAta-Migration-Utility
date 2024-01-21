
import {
  setTimeout 
}                      from "timers/promises"

import {
  networkInterfaces 
}                      from 'os';

import mongodb from 'mongodb'
const { MongoClient } = mongodb;

import MongoDBI        from '../../../node/dbi//mongodb/mongoDBI.js';
import MongoError      from '../../../node/dbi//mongodb/mongoException.js'
import MongoConstants  from '../../../node/dbi//mongodb/mongoConstants.js';

import Yadamu          from '../../core/yadamu.js';
import YadamuQALibrary from '../../lib/yadamuQALibrary.js'

class MongoQA extends YadamuQALibrary.qaMixin(MongoDBI) {
    
	get QA_COMPARE_DBNAME() { return 'YADAMU_QA' }
    
    static #_DBI_PARAMETERS
    
    static get DBI_PARAMETERS()  { 
       this.#_DBI_PARAMETERS = this.#_DBI_PARAMETERS || Object.freeze(Object.assign({},Yadamu.DBI_PARAMETERS,MongoConstants.DBI_PARAMETERS,Yadamu.QA_CONFIGURATION[MongoConstants.DATABASE_KEY] || {},{RDBMS: MongoConstants.DATABASE_KEY}))
       return this.#_DBI_PARAMETERS
    }
   
    get DBI_PARAMETERS() {
      return MongoQA.DBI_PARAMETERS
    }   
        
    constructor(yadamu,manager,connectionSettings,parameters) {
       super(yadamu,manager,connectionSettings,parameters)
    }
	
    async recreateSchema() {
        await this.use(this.parameters.TO_USER)
        await this.dropDatabase()
        await this.use(this.parameters.TO_USER)
    }

    classFactory(yadamu) {
      return new MongoQA(yadamu,this,this.connectionParameters,this.parameters)
    }
	
	async getConnectionID() {
	  let stack
      let operation
		  const currentOp = {
            currentOp : true
          , $all      : false
          , $ownOps   : true
          }
		  stack = new Error().stack
          const dbAdmin = await this.client.db('admin',{returnNonCachedInstance:true});  
          operation = `mongoClient.db('admin').command(${currentOp})`
          const ops = await dbAdmin.command(currentOp)
          const cmd = ops.inprog.filter((op) => {
			 // Filter by IP Address matching value from op.networkInterfaces()
			 return op.command.hasOwnProperty('currentOp')
		  })
		  const pid = cmd[0].client
		  return pid
	}
	
	async listCurrentOps() {
	
      const currentOp = {
        currentOp : true
      , $all      : true
	  , $ownOps   : true
      }
      const dbAdmin = await this.client.db('admin',{returnNonCachedInstance:true});  
      const ops = await dbAdmin.command(currentOp)
      console.log(ops)     
    }
	
    async scheduleTermination(pid,workerId) {
      let stack
      let operation
	  const tags = this.getTerminationTags(workerId,pid)
	  this.LOGGER.qa(tags,`Termination Scheduled.`);
	  setTimeout(this.yadamu.KILL_DELAY,pid,{ref : false}).then(async (pid) => {
        if (this.client !== undefined) {
		  
		  // this.listCurrentOps()
		  
		  this.LOGGER.log(tags,`Killing connection.`);
          const killClient = await new MongoClient(this.getMongoURL(),{ useUnifiedTopology: true});
          await killClient.connect();
          const dbAdmin = await killClient.db('admin',{returnNonCachedInstance:true});  
          const dropConnections = {
            dropConnections: 1
          , hostAndPort : [pid]
          }
		  stack = new Error().stack
          operation = `mongoClient.db('admin').command(${JSON.stringify(dropConnections)})`
          const res = await dbAdmin.command(dropConnections)
		  await killClient.close()
        }
        else {
          this.LOGGER.log(tags,`Unable to Kill Connection: Connection Pool no longer available.`);
        }
      }).catch((e) => {
		const cause = this.createDatabaseError(this.DRIVER_ID,e,stack,operation)
        this.LOGGER.handleException(tags,cause)
      })
    }
}

export { MongoQA as default }