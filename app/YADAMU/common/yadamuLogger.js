"use strict"

const {DatabaseError, OracleError}  = require('./yadamuError.js');

class YadamuLogger {
  
  constructor(outputStream,status) {
    this.os = outputStream;
    this.status = status;
	this.showStackTraces = process.env.YADAMU_SHOW_CAUSE && process.env.YADAMU_SHOW_CAUSE.toUpperCase() === 'TRUE'
  }

  switchOutputStream(os) {
	this.os = os;
  }

  write(msg) {
    this.os.write('**'+msg);
  }

  writeDirect(msg) {
    this.os.write(msg);
  }

  logNoTimestamp(args,msg) {
    this.os.write(`${args.map(function (arg) { return '[' + arg + ']'}).join('')}: ${msg}\n`)
  }
  
  log(args,msg) {
    this.os.write(`${new Date().toISOString()} ${args.map(function (arg) { return '[' + arg + ']'}).join('')}: ${msg}\n`)
  }
  
  info(args,msg) {
    args.unshift('INFO')
    this.log(args,msg)
  }
  
  warning(args,msg) {
    this.status.warningRaised = true;
    args.unshift('WARNING')
    this.log(args,msg)
  }

  error(args,msg) {
    this.status.errorRaised = true;
    args.unshift('ERROR')
    this.log(args,msg)
  }

  logDatabaseError(e) {
    this.os.write(`${e.message}\n`);
	this.os.write(`${e.stack}\n`)
	this.os.write(`SQL: ${e.sql}\n`);
    if (e instanceof OracleError) {
	  this.logOracleError(e)
	}
  }
  
  logOracleError(e) {
	this.os.write(`ErroNum: ${e.errorNum}\n`);
	this.os.write(`Offset: ${e.offset}\n`);
	this.os.write(`Args/Binds: ${JSON.stringify(e.args)}\n`);
	this.os.write(`OutputFormat/Rows: ${JSON.stringify(e.outputFormat)}\n`);
  } 
  
  logException(args,e) {

	if (e.yadamuAlreadyReported) {
      this.info(args,`Caught exception: "${e.message}"`);
	  return
   	}

    this.error(args,`Caught exception`);
	if (e instanceof DatabaseError) {
	  this.logDatabaseError(e);
	}
	else {
	  this.os.write(`${(e.stack ? e.stack : e)}\n`)
	}
	e.yadamuAlreadyReported = true;
  }
  
  logRejected(args,e) {
    this.status.warningRaised = true;
    args.unshift('REJECTED')
    this.log(args,`Cause`)
    this.os.write(`${(e.stack ? e.stack : e)}\n`)
    // console.log(new Error().stack);
  }

  trace(args,msg) {
    args.push('TRACE')
    this.log(args,msg)
  }
  
  loggingToConsole() {
    return this.os === process.stdout;
  }
  
  
  closeStream() {
  
    const self = this
	
	if (self.os.close) { 
      return new Promise(function(resolve,reject) {
        self.os.on('finish',function() { resolve() });
        self.os.close();
      })
	}
	
  }
  
  async close() {

    if (this.os !== process.stdout) {
      const closer = this.closeStream()
      await closer;
      this.os = process.stdout
    }    
  }
  
}

module.exports = YadamuLogger