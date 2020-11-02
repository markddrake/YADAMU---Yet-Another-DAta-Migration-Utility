"use strict"

const { Transform } = require('stream');
const { performance } = require('perf_hooks');

const Parser = require('../../clarinet/clarinet.js');

class JSONParser extends Transform {
 
  constructor(yadamuLogger, mode, exportFilePath) {

    super({objectMode: true });  

    this.yadamuLogger = yadamuLogger;
    this.mode = mode;
	this.exportFilePath = exportFilePath
	
	this.parser = Parser.createStream()
  
    this.tableList  = new Set();
    this.objectStack = [];
    this.dataPhase = false;     
    
    this.currentObject = undefined;
    this.chunks = [];

    this.jDepth = 0; 
	
	// Register for Parser Events.. The Parser events are used to create the objects that are 'pushed' out of this stream.
	
    this.registerEvents(this.parser)
	
  }

  registerEvents(parser) {
	
    parser.once('error',(err) => {
      this.yadamuLogger.handleException([`JSON_PARSER`,`JSON Parsing Error`,`"${this.exportFilePath}"`],err)
	  // How to stop the parser..
	  // this.parser.destroy(err)  
	  this.destroy(err);
  	  this.unpipe() 
	  // Swallow any further errors raised by the Parser
	  this.parser.on('error',(err) => {});
    })

    parser.on('end',(key) => {
	  this.endOfFile();
	})
    
    parser.on('key',(key) => {
      // this.yadamuLogger.trace([`${this.constructor.name}.onKey()`,`${this.jDepth}`,`"${key}"`],``);
      
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
    });

    parser.on('openobject',(key) => {
      // this.yadamuLogger.trace([`${this.constructor.name}.onOpenObject()`,`${this.jDepth}`,`"${key}"`],`ObjectStack:${this.objectStack}\n`);      
      
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
    });

    parser.on('openarray',() => {
      // this.yadamuLogger.trace([`${this.constructor.name}.onOpenArray()`,`${this.jDepth}`],'ObjectStack: ${this.objectStack}`);
      if (this.jDepth > 0) {
        this.objectStack.push(this.currentObject);
      }
      this.currentObject = [];
      this.jDepth++;
    });


    parser.on('valuechunk',(v) => {
      this.chunks.push(v);  
    });
       
    parser.on('value',(v) => {
      // this.yadamuLogger.trace([`${this.constructor.name}.onvalue()`,`${this.jDepth}`],`ObjectStack: ${this.objectStack}\n`);        
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
    });
      
    parser.on('closeobject',() => {
      // this.yadamuLogger.trace([`${this.constructor.name}.onCloseObject()`,`${this.jDepth}`],`\nObjectStack: ${this.objectStack}\nCurrentObject: ${this.currentObject}`);           
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
    });
   
    parser.on('closearray',() => {
      // this.yadamuLogger.trace([`${this.constructor.name}.onclosearray()`,`${this.jDepth}`],`\nObjectStack: ${this.objectStack}.\nCurrentObject:${this.currentObject}`);          
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

  _transform(data,enc,callback) {
    this.parser.write(data);
    callback();
  };

  async nextObject(name) {
    // this.yadamuLogger.trace([this.constructor.name,name],'nextObject()')
    this.push({[name]: this.currentObject})
  }
  
  async startTable(tableName) {
    try {
      // this.yadamuLogger.trace([this.constructor.name,tableName],'startTable()')
	  this.currentTable = tableName
	  this.readerStartTime = performance.now();
	  this.push({table: tableName})
    } catch(e) {
	  this.yadamuLogger.handleException(['JSONParser','START_TABLE',tableName],e)
	}
  }
  
  endTable() {
    // this.yadamuLogger.trace([this.constructor.name,this.currentTable],'endTable()')
    this.tableList.delete(this.currentTable);
	this.push({
      eod: {
	    startTime : this.readerStartTime
	  , endTime   : performance.now()
	  }
	})
  } 
  
  nextRow(data) {
	// this.yadamuLogger.trace([this.constructor.name,this.currentTable],'nextRow()')
    this.push({data:data});
  }
  
  endOfFile() {
	// this.yadamuLogger.trace([this.constructor.name],'eof()')
	this.push({eof: true})
  }
  
}
   
module.exports = JSONParser