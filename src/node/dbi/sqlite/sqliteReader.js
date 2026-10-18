import { 
  Readable 
}                        from 'stream';

/*
**
** SQLite has no native cursor stream. The sqlite3 binding materializes an entire result set
** via Database.all(), which is not viable for table sized result sets. Stepping a prepared
** Statement with get() returns one row per call, and undefined once the result set is
** exhausted, so SqliteReader wraps a prepared Statement in a Readable and pulls rows on
** demand. This gives the pipeline real backpressure instead of buffering the table in memory.
**
** Rows are emitted as positional arrays ordered by COLUMN_NAME_ARRAY. YadamuParser applies
** its transformations by column index (row[idx]), which is why the Postgres and MySQL drivers
** request "rowMode: array" / "rowsAsArray: true" from their bindings. The sqlite3 binding has
** no equivalent option, so the mapping is done here.
**
*/

class SqliteReader extends Readable {

  constructor(statement,columnNames) {
    super({objectMode: true})
    this.statement   = statement
    this.columnNames = columnNames
    this.exhausted   = false
    this.reading     = false
  }

  async fetchRows() {

    /*
    **
    ** Push rows until the consumer signals it has had enough (push() === false) or the
    ** statement is exhausted. _read() is called again once the consumer drains.
    **
    */

    while (true) {
      const row = await this.statement.get()
      if (row === undefined) {
        this.exhausted = true
        this.push(null)
        return
      }
      if (!this.push(this.columnNames.map((columnName) => row[columnName]))) {
        return
      }
    }
  }

  _read() {

    // A push() from fetchRows() can re-enter _read() while the loop is still running.

    if (this.exhausted || this.reading) return
    this.reading = true
    this.fetchRows().catch((e) => {
      this.destroy(e)
    }).finally(() => {
      this.reading = false
    })
  }

  async _destroy(err,callback) {
    try {
      await this.statement.finalize()
    } catch (e) {
      // Do not let a finalize failure mask the error that destroyed the stream.
      callback(err || e)
      return
    }
    callback(err)
  }

}

export { SqliteReader as default }
