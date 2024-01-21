
import { 
  performance 
}                from 'perf_hooks';

import { 
  Transform 
}                from 'stream';

import Parser    from '../../clarinet/clarinet.cjs';

import {
  IncompleteJSON 
}                from './fileException.js'

import DBIConstants                   from '../base/dbiConstants.js'

class JSONParser extends Transform {

  get LOGGER()               { return this._LOGGER }
  set LOGGER(v)              { this._LOGGER = v }
  get DEBUGGER()             { return this._DEBUGGER }
  set DEBUGGER(v)            { this._DEBUGGER = v }
  
  get PIPELINE_STATE()       { return this._PIPELINE_STATE }
  set PIPELINE_STATE(v)      { this._PIPELINE_STATE =  v }

  get STREAM_STATE()         { return this.PIPELINE_STATE[DBIConstants.PARSER_STREAM_ID] }
  set STREAM_STATE(v)        { this.PIPELINE_STATE[DBIConstants.PARSER_STREAM_ID] = v }
	    
  constructor(mode, exportFilePath, pipelineState, yadamuLogger) {

    super({objectMode: true });  
    
    this.mode = mode;
	this.exportFilePath = exportFilePath
	this.PIPELINE_STATE = pipelineState
    this.STREAM_STATE = {}

    this.LOGGER = yadamuLogger;
	
	this.parser = Parser.createStream()
    this.parseComplete = false;
	this.tableState = {}
	
    this.tableList  = new Set();
    this.objectStack = [];
    this.dataPhase = false;     
    
    this.currentObject = undefined
    this.chunks = [];

    this.jDepth = 0; 
	
	// Register for Parser Events.. The Parser events are used to create the objects that are 'pushed' out of this stream.
	
    this.registerEvents(this.parser)
	
  }

  registerEvents(parser) {
	
    parser.once('error',(err) => {
      this.LOGGER.handleException([`JSON_PARSER`,`JSON Parsing Error`,`"${this.exportFilePath}"`],err)
	  // How to stop the parser..
	  // this.parser.destroy(err)  
	  this.destroy(err);
  	  this.unpipe() 
	  // Swallow any further errors raised by the Parser
	  this.parser.on('error',(err) => {});
    }).on('end',(key) => {
	  this.endOfFile();
	}).on('key',(key) => {
      // this.LOGGER.trace([`${this.constructor.name}.onKey()`,`${this.jDepth}`,`"${key}"`],``);
      
      switch (this.jDepth){
        case 1:
          if (key === 'data'){ 
            this.dataPhase = true;
          }
          break;
        case 2:
          if (this.dataPhase) {
			this.startTable(key)
          }
          break;
        default:
      }
      // Push the current object onto the stack and the current object to the key
      this.objectStack.push(this.currentObject);
      this.currentObject = key;
    }).on('openobject',(key) => {
      // this.LOGGER.trace([`${this.constructor.name}.onOpenObject()`,`${this.jDepth}`,`"${key}"`],`ObjectStack:${this.objectStack}\n`);      
      
      if (this.jDepth > 0) {
        this.objectStack.push(this.currentObject);
      }
         
      switch (this.jDepth) {
        case 0:
          // Push the completed first level object/array downstream. Replace the current top level object with an empty object of the same type.
          if (this.currentObject !== undefined) {
            if (this.currentObject.metadata) {
              this.tableList = new Set(Object.keys(this.currentObject.metadata));
            }
            this.push(this.currentObject);
          }  
          break;
        case 1:
          if ((this.dataPhase) && (key != undefined)) {
   			this.startTable(key)
          }
          break;
        default:
      }
      // If the object has a key put the object on the stack and set the current object to the key. 
      this.currentObject = {}
      this.jDepth++;
      if (key !== undefined) {
        this.objectStack.push(this.currentObject);
        this.currentObject = key;
      }
    }).on('openarray',() => {
      // this.LOGGER.trace([`${this.constructor.name}.onOpenArray()`,`${this.jDepth}`],'ObjectStack: ${this.objectStack}`);
      if (this.jDepth > 0) {
        this.objectStack.push(this.currentObject);
      }
      this.currentObject = [];
      this.jDepth++;
    }).on('valuechunk',(v) => {
      this.chunks.push(v);  
    }).on('value',(v) => {
      // this.LOGGER.trace([`${this.constructor.name}.onvalue()`,`${this.jDepth}`],`ObjectStack: ${this.objectStack}\n`);        
      if (this.chunks.length > 0) {
        this.chunks.push(v);
        v = this.chunks.join('');
        this.chunks = []
      }
      
      if (Array.isArray(this.currentObject)) {
          // currentObject is an ARRAY. We got a value so add it to the Array
          this.currentObject.push(v);
      }
      else {
          // currentObject is an Key. We got a value so fetch the parent object and add the KEY:VALUE pair to it. Parent Object becomes the Current Object.
          const parentObject = this.objectStack.pop();
          parentObject[this.currentObject] = v;
          this.currentObject = parentObject;
      }
    }).on('closeobject',() => {
      // this.LOGGER.trace([`${this.constructor.name}.onCloseObject()`,`${this.jDepth}`],`\nObjectStack: ${this.objectStack}\nCurrentObject: ${this.currentObject}`);           
      this.jDepth--;

      switch (this.jDepth){
		case 0:
          this.endOfFile();
          break;
        case 1:
          // Push the completed first level object/array downstream. Replace the current top level object with an empty object of the same type.
          if (this.currentObject.metadata) {
            this.tableList = new Set(Object.keys(this.currentObject.metadata));
          }
          const key = this.objectStack.pop()
          this.objectStack.pop()
          if (this.dataPhase) {
            this.dataPhase = false;
          }
          else {
		    if ((this.mode !== 'DATA_ONLY') || (key !== 'ddl')) {
		      this.nextObject(key)
		    }
          }
          this.currentObject = {};
          break;
        default:
          // An object can belong to an Array or a Key
          if (this.objectStack.length > 0) {
            let owner = this.objectStack.pop()
            let parentObject = undefined;
            if (Array.isArray(owner)) {   
              parentObject = owner;
              parentObject.push(this.currentObject);
            }    
            else {
              parentObject = this.objectStack.pop()
              if (!this.emptyObject) {
                parentObject[owner] = this.currentObject;
              }
            }   
            this.currentObject = parentObject;
          }
      }
    }).on('closearray',() => {
      // this.LOGGER.trace([`${this.constructor.name}.onclosearray()`,`${this.jDepth}`],`\nObjectStack: ${this.objectStack}.\nCurrentObject:${this.currentObject}`);          
      this.jDepth--;

      let skipObject = false;

      switch (this.jDepth){
		case 0:
          this.endOfFile()
		  break;
        case 1:
          // Push the completed first level object/array downstream. Replace the current top level object with an empty object of the same type.
		  const key = this.objectStack.pop()
          this.objectStack.pop()
		  if ((this.mode !== 'DATA_ONLY') || (key !== 'ddl')) {
 		    this.push({[key]: this.currentObject});
		  }
          this.currentObject = [];
          break;
        case 2:
          if (this.dataPhase) {
			this.endTable()
          }
          break;
        case 3:
          if (this.dataPhase) {
            // this.push({ data : this.currentObject});
			this.nextRow(this.currentObject)
			skipObject = true;
          }
      }

      // An Array can belong to an Array or a Key
      if (this.objectStack.length > 0) {
        let owner = this.objectStack.pop()
        let parentObject = undefined;
        if (Array.isArray(owner)) {   
          parentObject = owner;
          if (!skipObject) {
            parentObject.push(this.currentObject);
          }
        }    
        else {
          parentObject = this.objectStack.pop()
          if (!skipObject) {
            parentObject[owner] = this.currentObject;
          }
        }
        this.currentObject = parentObject;
      }   
    });     
  }     

  async nextObject(name) {
    // this.LOGGER.trace([this.constructor.name,name],'nextObject()')
    this.push({[name]: this.currentObject})
  }
  
  async startTable(tableName) {
    try {
      // this.LOGGER.trace([this.constructor.name,tableName],'startTable()')
	  this.currentTable = tableName
	  this.tableState = {
		startTime : performance.now()
      , parsed    : 0
	  }
	  this.push({table: tableName})
    } catch(e) {
	  this.LOGGER.handleException(['JSONParser','START_TABLE',tableName],e)
	}
  }
  
  endTable() {
    // this.LOGGER.trace([this.constructor.name,this.currentTable],'endTable()')
    this.tableList.delete(this.currentTable);
	// Snapshot the table start and end times.
	this.tableState.endTime = performance.now()
	this.push({eod: this.tableState})
  } 
  
  nextRow(data) {
	// this.LOGGER.trace([this.constructor.name,this.currentTable],'nextRow()')
	this.tableState.parsed++;
	this.push({data:data});
  }
  
  endOfFile() {
	// this.LOGGER.trace([this.constructor.name],'eof()')
	this.parseComplete = true
	this.push({eof: true})
  }
  
  _transform(data,enc,callback) {
	// console.log("\n###CHUNK###:",data.toString())
	try {
	  this.parser.write(data);
	  // Current logic may push() one or more times after the callback() has been invoked..
      callback();
	} catch (e) { 
   	  this.PIPELINE_STATE[DBIConstants.PARSER_STREAM_ID].error = e
	  callback(e) 
	}
  }
  
  _flush(callback) {
	
    if (!this.parseComplete) {
	  const parserError = new IncompleteJSON(this.exportFilePath)
   	  this.PIPELINE_STATE[DBIConstants.PARSER_STREAM_ID].error = parserError
	  callback(parserError)
    }
	else {
	  callback()
	}
  }
	 
  
}
   
export { JSONParser as default }