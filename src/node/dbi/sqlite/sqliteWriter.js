import { performance } from 'perf_hooks';

import YadamuWriter from '../base/yadamuWriter.js';

/*
**
** Batch Layout:
**
** YadamuOutputManager.cacheRow() pushes an array containing the values for a single row onto the batch, so a
** batch is an array of rows and rowCount is the number of rows in the batch.
**
** Batch Mode:
**
** SQLite limits the number of bind variables that can be supplied in a single SQL statement
** (SQLITE_MAX_VARIABLE_NUMBER), so multi row VALUES clauses cannot be used for large batches. Batch mode
** prepares the DML statement once and reuses it for each row in the batch. The statement is retained between
** batches and finalized when the writer finishes. (Unfinalized statements cause connection.close() to fail
** with SQLITE_BUSY, so statements are finalized in doFinal() and doDestroy(), covering both the success and
** error paths.)
**
** A Save Point is established before the batch is written and released after it completes. If an error occurs
** part way through the batch the Save Point is restored, undoing any rows already inserted, the statement is
** finalized and the writer switches to Iterative mode, reprocessing the batch from the start.
**
** Iterative Mode:
**
** Rows are written one at a time using dbi.executeSQL(), which prepares, runs and finalizes a statement for
** each row. Errors are handled by handleIterativeError() which logs the failed row and continues with the
** next row until TABLE_MAX_ERRORS is exceeded.
**
*/

class SqliteWriter extends YadamuWriter {

  constructor(dbi, tableName, pipelineState, status, yadamuLogger) {
    super(dbi, tableName, pipelineState, status, yadamuLogger)
    this.statement = null
  }

  getMetrics() {
    const results = super.getMetrics()
    results.insertMode = this.tableInfo.insertMode
    return results;
  }

  reportBatchError(operation,cause,batch) {
    super.reportBatchError(operation,cause,batch[0] || [],batch[batch.length-1] || [])
  }

  async finalizeStatement() {
    if (this.statement === null) return
    const statement = this.statement
    this.statement = null
    try {
      await statement.finalize()
    } catch (e) {
      this.LOGGER.warning([this.dbi.DATABASE_VENDOR,this.tableName,this.tableInfo?.insertMode,'finalizeStatement()'],`Failed to finalize prepared statement. Cause: ${e.message}`)
    }
  }

  async doFinal() {
    await super.doFinal()
    await this.finalizeStatement()
  }

  async doDestroy(err) {
    await this.finalizeStatement()
    await super.doDestroy(err)
  }

  async _writeBatch(batch, rowCount) {

    if (this.tableInfo.insertMode === 'Batch') {
      try {
        await this.dbi.createSavePoint();
        if (this.statement === null) {
          this.statement = await this.dbi.connection.prepare(this.tableInfo.dml)
        }
        for (let row = 0; row < rowCount; row++) {
          await this.statement.run(batch[row])
        }
        this.endTime = performance.now();
        await this.dbi.releaseSavePoint();
        this.adjustRowCounts(rowCount);
        this.releaseBatch(batch)
        return this.skipTable
      } catch (cause) {
        this.reportBatchError(`INSERT MANY`,cause,batch)
        await this.dbi.restoreSavePoint(cause);
        await this.finalizeStatement()
        this.LOGGER.warning([this.dbi.DATABASE_VENDOR,this.tableName,this.tableInfo.insertMode],`Switching to Iterative mode.`);
        this.dbi.resetExceptionTracking()
        this.tableInfo.insertMode = 'Iterative'
      }
    }

    for (let row = 0; row < rowCount; row++) {
      try {
        await this.dbi.executeSQL(this.tableInfo.dml,batch[row])
        this.adjustRowCounts(1);
      } catch (cause) {
        this.handleIterativeError(`INSERT ONE`,cause,row,batch[row]);
        if (this.skipTable) {
          break;
        }
      }
    }

    this.endTime = performance.now();
    this.releaseBatch(batch)
    return this.skipTable
  }

}

export { SqliteWriter as default }