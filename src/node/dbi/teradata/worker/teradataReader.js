
import {
  Readable
}                      from 'stream';

import YadamuLibrary                  from '../../../lib/yadamuLibrary.js'

class TeradataReader extends Readable {

  /*
  **
  ** Rows are pulled from the worker thread one batch at a time (see worker/teradataWorker.js). While one batch is being pushed
  ** the next is being fetched, so at most two batches are held in memory, however slowly the rows are consumed.
  **
  */

    constructor(dbi,sqlStatement,tableName,fetchSize) {
      super({objectMode:true})
      this.dbi = dbi
      this.sqlStatement = sqlStatement
      this.tableName = tableName
      this.recordsRead = 0
	  this.fetchSize = fetchSize;

	  this.queryId = undefined
	  this.rows = []
	  this.rowIndex = 0
	  this.streamComplete = false
	  this.nextBatch = undefined
    }

  fetchNextBatch() {
	// Settle with the error rather than reject, so a fetch that fails before _read() waits for it is not an unhandled rejection.
	return this.dbi.enqueueTask({action : "next", sql: this.sqlStatement, queryId: this.queryId, batchSize: this.fetchSize}).catch((e) => { return e })
  }

  setBatch(response) {
	this.rows = response.rows
	this.rowIndex = 0
	this.recordsRead+= response.rows.length
	this.streamComplete = response.streamComplete
	this.nextBatch = this.streamComplete ? undefined : this.fetchNextBatch()
  }

  async doConstruct() {
    const response = await this.dbi.enqueueTask({action : "query", sql: this.sqlStatement, batchSize: this.fetchSize})
	this.queryId = response.id
	this.setBatch(response)
  }

  async doRead() {
	while (true) {
	  while (this.rowIndex < this.rows.length) {
		if (!this.push(this.rows[this.rowIndex++])) {
		  return
		}
	  }
	  if (this.streamComplete) {
		this.push(null)
		return
	  }
	  const response = await this.nextBatch
	  if (this.destroyed) {
		return
	  }
	  if (response instanceof Error) {
		this.streamComplete = true
		throw response
	  }
	  this.setBatch(response)
	}
  }

  _construct(callback) {
    this.doConstruct().then(() => {callback()}).catch((e) => {callback(e)})
  }

  _read() {
    this.doRead().then(YadamuLibrary.NOOP).catch((e) => {this.destroy(e)})
  }

  async doDestroy(e) {
    /// console.log('Read','Destroy',this.recordsRead);
	if ((this.queryId !== undefined) && !this.streamComplete) {
	  // Stopped early. Close the worker's cursor. The worker finishes any fetch already in progress first, and its rows are discarded.
	  this.streamComplete = true
	  this.rows = []
	  await this.dbi.enqueueTask({action : "cancel", sql: this.sqlStatement, queryId: this.queryId}).catch(YadamuLibrary.NOOP)
	}
  }

  _destroy(e,callback) {
	this.doDestroy().then(() => {callback(e)}).catch((err) => {err.cause = e; callback(err)})
  }
}

export { TeradataReader as default }
