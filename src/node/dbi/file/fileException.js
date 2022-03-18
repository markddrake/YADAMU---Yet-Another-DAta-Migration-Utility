"use strict"

import {
  YadamuError,
  DatabaseError
}                  from '../../core/yadamuException.js'

class FileError extends DatabaseError {
  constructor(driverId,cause,stack,file) {
    super(driverId,cause,stack,file);
	this.path = this.sql
	delete this.sql
  }
}

class FileNotFound extends FileError {
  constructor(driverId,cause,stack,file) {
    super(driverId,cause,stack,file);
  }
}

class DirectoryNotFound extends FileError {
  constructor(driverId,cause,stack,file) {
    super(driverId,cause,stack,file);
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