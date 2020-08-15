"use strict" 

const { Transform } = require('stream');
const Readable = require('stream').Readable;
// const clarinet = require('clarinet');
const clarinet = require('../../clarinet/clarinet.js');
const { performance } = require('perf_hooks');

class JSONParser extends Transform {
  
  get MAX_ERRORS() { return 10 }
  
  constructor(yadamuLogger, mode, path, options) {
      
    super({objectMode: true });  
    this.yadamuLogger = yadamuLogger;
	this.mode = mode;
    
	this.rowsRead = 0;
	this.startTime = undefined;
	this.endTime = undefined;

    this.parser = clarinet.createStream();
    
    this.parser.once('error',(err) => {
      yadamuLogger.handleException([`JSON_PARSER`,`Invalid JSON Document`,`"${path}"`],err)
	  // How to stop the parser..
	  // this.parser.destroy(err)  
	  this.destroy(err);
  	  this.unpipe() 
	  // Swallow any further errors raised by the Parser
	  this.parser.on('error',(err) => {});
    })
    
    this.parser.on('key',(key) => {
      // Push the current object onto the stack and the current object to the key

      // this.yadamuLogger.trace([`${this.constructor.name}.onKey()`,`${this.jDepth}`,`"${key}"`],``);
      
      this.objectStack.push(this.currentObject);
      this.currentObject = key;
    });

    this.parser.on('openobject',(key) => {
      // If the object has a key put the object on the stack and set the current object to the key. 

      // this.yadamuLogger.trace([`${this.constructor.name}.onOpenObject()`,`${this.jDepth}`,`"${key}"`],`ObjectStack:${this.objectStack}\n`);      
      
      if (this.jDepth > 0) {
        this.objectStack.push(this.currentObject);
      }
         
      this.currentObject = {}
      this.jDepth++;
      if (key !== undefined) {
        this.objectStack.push(this.currentObject);
        this.currentObject = key;
      }
    });

    this.parser.on('openarray',() => {
      // this.yadamuLogger.trace([`${this.constructor.name}.onOpenArray()`,`${this.jDepth}`],`ObjectStack: ${this.objectStack}`);
      if (this.jDepth > 0) {
        this.objectStack.push(this.currentObject);
      }
      this.currentObject = [];
      this.jDepth++;
    });


    this.parser.on('valuechunk',(v) => {
      this.chunks.push(v);  
    });
       
    this.parser.on('value',(v) => {
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
      
    this.parser.on('closeobject',() => {
      // this.yadamuLogger.trace([`${this.constructor.name}.onCloseObject()`,`${this.jDepth}`],`\nObjectStack: ${this.objectStack}\nCurrentObject: ${JSON.stringify(this.currentObject)}`);           
      this.jDepth--;

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
    });
   
    this.parser.on('closearray',() => {
      // this.yadamuLogger.trace([`${this.constructor.name}.onclosearray()`,`${this.jDepth}`],`\nObjectStack: ${this.objectStack}.\nCurrentObject:${JSON.stringify(this.currentObject)}`);          
      this.jDepth--;

      let skipObject = false;
    
      switch (this.jDepth){
		case 0:
   	      this.endTime = performance.now();
		  this.push(null);
          break;
        case 1:
          this.push({ data : this.currentObject});
	      this.rowsRead++;
          skipObject = true;
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
    
	
    this.tableList  = new Set();
    this.objectStack = [];
    
    this.currentObject = undefined;
    this.chunks = [];

    this.jDepth = 0; 
	
	// Push a table entry before sending data. Ensure that the Writer waits for DDL to complete before writing data.
	// this.push({table: this.currentTable})
  }     
     
  checkState() {
    if (this.tableList.size === 0) {
      return false;
    }
    else {
      this.tableList.forEach((table) => {
        this.yadamuLogger.warning([`${this.constructor.name}`,`"${table}"`],`No records found - Possible corrupt or truncated import file.\n`);
      })
      return true;
    }
  };
   
  _transform(data,enc,callback) {
	this.parser.write(data);
    callback();
  };
  
  getCounter() {
    return this.rowsRead
  }

}

module.exports = JSONParser;
