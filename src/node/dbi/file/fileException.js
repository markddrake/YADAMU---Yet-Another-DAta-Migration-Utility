
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

export {
  FileError
, FileNotFound
, DirectoryNotFound
, IncompleteJSON
}