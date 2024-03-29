"use strict"

import { Transform } from 'stream';
import { performance } from 'perf_hooks';

import Parser from '../../clarinet/clarinet.cjs';

class RowCounter extends Transform {

  get ROW_COUNT()   { return this._ROW_COUNT }

  get LOGGER()             { return this._LOGGER }
  set LOGGER(v)            { this._LOGGER = v }
  
  get DEBUGGER()           { return this._DEBUGGER }
  set DEBUGGER(v)          { this._DEBUGGER = v }

  constructor(exportFilePath,yadamuLogger) {

    super();  

	this.exportFilePath = exportFilePath
    this.LOGGER = yadamuLogger;
	
	this.parser = Parser.createStream()
  
	// Register for Parser Events.. The Parser events are used to create the objects that are 'pushed' out of this stream.
	
    this.registerEvents(this.parser)
	
    this.jDepth = 0; 
	this._ROW_COUNT = 0;
	
  }
  
  registerEvents(parser) {
  
    parser.once('error',(err) => {
      this.LOGGER.handleException([`JSON_PARSER`,`Invalid JSON Document`,`"${exportFilePath}"`],err)
	  // parser.destroy(err);
  	  // Swallow any further errors raised by the Parser
	  // parser.on('error',(err) => {});
    }).on('openobject',(key) => {
      this.jDepth++;
    }).on('openarray',() => {
      this.jDepth++;
    }).on('closeobject',() => {
      // this.LOGGER.trace([`${this.constructor.name}.onCloseObject()`,`${this.jDepth}`],`\nObjectStack: ${this.objectStack}\nCurrentObject: ${JSON.stringify(this.currentObject)}`);           
      this.jDepth--;
    }).on('closearray',() => {
	  // this.LOGGER.trace([`${this.constructor.name}.onclosearray()`,`${this.jDepth}`],`\nObjectStack: ${this.objectStack}.\nCurrentObject:${JSON.stringify(this.currentObject)}`);          
      this.jDepth--;
      switch (this.jDepth){
        case 1:
		  this._ROW_COUNT++
		 
      }
    });  
  }

  _transform(data,enc,callback) {
	// console.log("\n###CHUNK###:",data.toString())
    this.parser.write(data);
    callback();
  };

}

export { RowCounter as default }