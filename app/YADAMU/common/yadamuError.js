"use strict"

class YadamuError extends Error {
  constructor(message) {
    super(message);
   // Ensure the name of this error is the same as the class name
    this.name = this.constructor.name;
    Error.captureStackTrace(this, this.constructor);
  }
}

class CommandLineError extends YadamuError {
  constructor(message) {
    super(message);
  }
}

class ConfigurationFileError extends YadamuError {
  constructor(message) {
    super(message);
  }
}

module.exports = {
  YadamuError
, CommandLineError  
, ConfigurationFileError
}