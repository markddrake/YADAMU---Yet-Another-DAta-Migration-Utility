
import {
  YadamuError,
  DatabaseError
}                  from '../../core/yadamuException.js'

class FileError extends DatabaseError {
  constructor(dbi,cause,stack,file) {
    super(dbi,cause,stack,file);
	this.path = this.sql
	delete this.sql
  }

  getDiagnostics() {
	return [`PATH: "${this.path}"`]
  }
}

class FileNotFound extends FileError {
  constructor(dbi,cause,stack,file) {
    super(dbi,cause,stack,file);
  }
}

class DirectoryNotFound extends FileError {
  constructor(dbi,cause,stack,file) {
    super(dbi,cause,stack,file);
  }
}

class IncompleteJSON extends YadamuError {
  constructor(file) {
	super(`JSON parsing failed: Incomplete JSON Document. Unexpected EOF encountered while parsing "${file}"`)
  }
}

// Malformed content in a JSON data file. Deliberately not a SyntaxError, which YadamuOutcome reports as an invalid request.
class InvalidJSON extends YadamuError {
  constructor(message,cause) {
	super(`JSON parsing failed: ${message}`,cause ? {cause: cause} : undefined)
  }
}

export {
  FileError
, FileNotFound
, DirectoryNotFound
, IncompleteJSON
, InvalidJSON
}