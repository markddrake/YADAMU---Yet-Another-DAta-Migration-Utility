"use strict"

import { Transform } from 'stream';
import { performance } from 'perf_hooks';

import Parser from '../../node/clarinet/clarinet.cjs';

class RowCounter extends Transform {

  get ROW_COUNT()   { return this._ROW_COUNT }

  constructor(exportFilePath,yadamuLogger) {

    super();  

	this.exportFilePath = exportFilePath
    this.yadamuLogger = yadamuLogger;
	
	this.parser = Parser.createStream()
  
	// Register for Parser Events.. The Parser events are used to create the objects that are 'pushed' out of this stream.
	
    this.registerEvents(this.parser)
	
    this.jDepth = 0; 
	this._ROW_COUNT = 0;
	
  }
  
  registerEvents(parser) {
  
    parser.once('error',(err) => {
      this.yadamuLogger.handleException([`JSON_PARSER`,`Invalid JSON Document`,`"${exportFilePath}"`],err)
	  // parser.destroy(err);
  	  // Swallow any further errors raised by the Parser
	  // parser.on('error',(err) => {});
    }).on('openobject',(key) => {
      this.jDepth++;
    }).on('openarray',() => {
      this.jDepth++;
    }).on('closeobject',() => {
      // this.yadamuLogger.trace([`${this.constructor.name}.onCloseObject()`,`${this.jDepth}`],`\nObjectStack: ${this.objectStack}\nCurrentObject: ${JSON.stringify(this.currentObject)}`);           
      this.jDepth--;
    }).on('closearray',() => {
	  // this.yadamuLogger.trace([`${this.constructor.name}.onclosearray()`,`${this.jDepth}`],`\nObjectStack: ${this.objectStack}.\nCurrentObject:${JSON.stringify(this.currentObject)}`);          
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