"use strict"

class YadamuLogger {
  
  constructor(outputStream,status) {
    this.os = outputStream;
    this.status = status;
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
  
  logException(args,e) {
    this.error(args,`Caught exception`);
    this.os.write(`${(e.stack ? e.stack : e)}\n`)
    // console.log(new Error().stack);
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

    return new Promise(function(resolve,reject) {
      self.os.on('finish',function() { resolve() });
      self.os.close();
    })
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