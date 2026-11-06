
import {
  parentPort,
  workerData
}                                     from 'worker_threads';

import TeradataConnection             from "teradatasql/lib/teradata-connection.js";

import TeradataConstants              from '../teradataConstants.js'

/*
**
** Runs the synchronous teradatasql calls for one TeradataDBI (see worker/teradataDBI.js).
**
** Every message carries an id, and the response echoes it. Messages are processed one at a time, so the worker is never inside
** more than one driver call. Reads are pulled: 'query' opens the cursor and returns the first batch, then each 'next' returns one
** more batch. Nothing is sent that the main thread did not ask for. 'cancel' closes the cursor of a reader that stopped early.
**
** The main thread must not terminate the worker while it is inside a koffi call (Node aborts with
** "FATAL ERROR: Error::ThrowAsJavaScriptException napi_throw"). When the process exits the main thread sets STOP and blocks
** until the worker sets CLOSED: the worker finishes its current call, closes its cursor and connection, and makes no further driver calls.
**
*/

const STOP   = TeradataConstants.WORKER_STOP
const CLOSED = TeradataConstants.WORKER_CLOSED

class TeradataWorker {

  constructor() {
    this.state = new Int32Array(workerData.state)
    this.connection = undefined
    this.readCursor = undefined
    this.queryId = undefined
    this.configureActions()
  }

  configureActions() {
    parentPort.on('message', (message) => {
      const response = this.processMessage(message)
      parentPort.postMessage({action: message.action, id: message.id, ...response})
    })
  }

  processMessage(message) {
    try {
      if (Atomics.load(this.state,STOP) === 1) {
        throw new Error('Teradata worker is shutting down.')
      }
      return this.performAction(message)
    } catch (e) {
      return {success: false, cause: {name: e.name, message: e.message, stack: e.stack}}
    } finally {
      if (Atomics.load(this.state,STOP) === 1) {
        this.shutdown()
      }
    }
  }

  performAction(message) {
    switch (message.action) {
      case 'console' :
        this.consoleChannel = message.payload
        // this.consoleChannel.postMessage('Console Channel Active');
        return {success: true}
      case 'connect':
        this.connect(message.connectionProperties)
        return {success: true}
      case 'executeSQL':
        return {success: true, rows: this.executeSQL(message.sql, message.args)}
      case 'query':
        return this.query(message.id, message.sql, message.batchSize)
      case 'next':
        return this.next(message.queryId, message.batchSize)
      case 'cancel':
        this.closeQuery(message.queryId)
        return {success: true}
      case 'disconnect':
        this.disconnect()
        return {success: true}
      default:
        throw new Error(`Unknown action "${message.action}".`)
    }
  }

  connect(connectionProperties) {
    this.connection = new TeradataConnection.TeradataConnection();
    this.connection.connect(connectionProperties);
    this.freeFetchedRowBuffers(this.connection)
  }

  freeFetchedRowBuffers(connection) {

    // teradatasql 20.0.20 (the move from ffi-napi to koffi) dropped the jsgoFreePointer() call that freed the buffer
    // jsgoFetchRow returns, so every row fetched leaks its serialized size in native memory (still true in 20.0.72).
    // fetchmany_() has copied the buffer before it returns, so free each buffer on the next fetch, and the last one
    // when the rows are closed. The function table is per connection, so this only affects this connection.

    const lib = connection.lib
    const jsgoFreePointer = connection.gosideLib.func("void jsgoFreePointer(uint64_t, void*)")
    const jsgoFetchRow = lib.jsgoFetchRow
    const jsgoCloseRows = lib.jsgoCloseRows
    let rowBuffer = null

    const freeRowBuffer = (uLog) => {
      if (rowBuffer !== null) {
        jsgoFreePointer(uLog,rowBuffer)
        rowBuffer = null
      }
    }

    lib.jsgoFetchRow = (uLog,uRowsHandle,outputPtrPtr,columnValuesByteCountPtr,byteArrayPtrPtr,nDesiredRowCount) => {
      freeRowBuffer(uLog)
      jsgoFetchRow(uLog,uRowsHandle,outputPtrPtr,columnValuesByteCountPtr,byteArrayPtrPtr,nDesiredRowCount)
      rowBuffer = byteArrayPtrPtr[0] ?? null
    }

    lib.jsgoCloseRows = (uLog,uRowsHandle,outputPtrPtr) => {
      freeRowBuffer(uLog)
      jsgoCloseRows(uLog,uRowsHandle,outputPtrPtr)
    }
  }

  query(queryId,sqlStatement,batchSize) {

    // Only one read is open at a time. Close the cursor of a reader that went away without cancelling.
    this.closeQuery()

    const cursor = this.connection.cursor()
    try {
      cursor.execute(sqlStatement);
      const rows = cursor.fetchmany(batchSize)
      const streamComplete = rows.length < batchSize
      if (streamComplete) {
        this.closeCursor(cursor)
      }
      else {
        this.readCursor = cursor
        this.queryId = queryId
      }
      return {success: true, streamComplete: streamComplete, rows: rows}
    } catch (e) {
      this.closeCursor(cursor)
      throw e
    }
  }

  next(queryId,batchSize) {
    if ((this.readCursor === undefined) || (queryId !== this.queryId)) {
      throw new Error(`Query ${queryId} is not open.`)
    }
    try {
      const rows = this.readCursor.fetchmany(batchSize)
      const streamComplete = rows.length < batchSize
      if (streamComplete) {
        this.closeQuery()
      }
      return {success: true, streamComplete: streamComplete, rows: rows}
    } catch (e) {
      this.closeQuery()
      throw e
    }
  }

  closeQuery(queryId) {
    // Without a queryId, close whatever read is open.
    if ((this.readCursor !== undefined) && ((queryId === undefined) || (queryId === this.queryId))) {
      const cursor = this.readCursor
      this.readCursor = undefined
      this.queryId = undefined
      this.closeCursor(cursor)
    }
  }

  closeCursor(cursor) {
    // Called on error and cancel paths, where the cursor (or its connection) may already be broken. The original error is the one that matters.
    try {
      cursor.close()
    } catch (e) { /* Ignore */ }
  }

  executeSQL(sqlStatement,args) {
    const cursor = this.connection.cursor()
    try {
      cursor.execute(sqlStatement,args)
      return cursor.fetchall()
    } finally {
      this.closeCursor(cursor)
    }
  }

  disconnect() {
    this.closeQuery()
    const connection = this.connection
    this.connection = undefined
    try {
      if (connection) {
        connection.close();
      }
    } finally {
      Atomics.store(this.state,CLOSED,1)
      Atomics.notify(this.state,CLOSED)
    }
  }

  shutdown() {
    try {
      this.disconnect()
    } catch (e) { /* The process is exiting */ }
  }
}

const teradataWorker = new TeradataWorker()
