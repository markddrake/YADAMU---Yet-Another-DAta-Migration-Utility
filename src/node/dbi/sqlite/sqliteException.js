import { DatabaseError } from '../../core/yadamuException.js'

class SqliteError extends DatabaseError {

  constructor(cause, stack, sql) {
    super(cause, stack, sql);
  }

  lostConnection() {
    return (this.cause.message && this.cause.message.includes('SQLITE_CANTOPEN')) ||
           (this.cause.message && this.cause.message.includes('SQLITE_IOERR'));
  }

  brokenTransaction() {
    return (this.cause.message && this.cause.message.includes('SQLITE_BUSY'));
  }
}

export { SqliteError as default }
