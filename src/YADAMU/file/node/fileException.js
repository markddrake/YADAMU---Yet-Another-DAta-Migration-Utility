"use strict"

import {DatabaseError} from '../../common/yadamuException.js'

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
    super(cdriverId,ause,stack,file);
  }
}

export {
  FileError
, FileNotFound
, DirectoryNotFound
}