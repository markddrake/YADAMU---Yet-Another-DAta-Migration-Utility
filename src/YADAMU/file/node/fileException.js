"use strict"

const {DatabaseError} = require('../../common/yadamuException.js')

class FileError extends DatabaseError {
  constructor(cause,stack,file) {
    super(cause,stack,file);
	this.path = this.sql
	delete this.sql
  }
}

class FileNotFound extends FileError {
  constructor(cause,stack,file) {
    super(cause,stack,file);
  }
}

class DirectoryNotFound extends FileError {
  constructor(cause,stack,file) {
    super(cause,stack,file);
  }
}

module.exports = {
  FileError
, FileNotFound
, DirectoryNotFound
}