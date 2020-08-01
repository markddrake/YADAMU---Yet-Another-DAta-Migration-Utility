"use strict" 

const { Transform } = require('stream');
const Readable = require('stream').Readable;
// const clarinet = require('clarinet');
const clarinet = require('../../clarinet/clarinet.js');
const { performance } = require('perf_hooks');

class JSONParser extends Transform {
  
  constructor(yadamuLogger, mode, options) {

    super({objectMode: true });  
   
    this.mode = mode;
  
	this.rowsRead = 0;
	this.startTime = undefined;
	this.endTime = undefined;
	
    this.yadamuLogger = yadamuLogger;

    this.parser = clarinet.createStream();
    
    this.parser.on('error',(err) => {
      yadamuLogger.handleException([`${this.constructor.name}.onError()`],err)
    })
    
    this.parser.on('key',(key) => {
      // this.yadamuLogger.trace([`${this.constructor.name}.onKey()`,`${this.jDepth}`,`"${key}"`],``);
      
      switch (this.jDepth){
        case 1:
          if (key === 'data'){ 
            this.dataPhase = true;
          }
          break;
        case 2:
          if (this.dataPhase) {
            this.currentTable = key;                
            this.push({ table : key});
			this.startTime = performance.now();
			this.rowsRead = 0;
          }
          break;
        default:
      }
      // Push the current object onto the stack and the current object to the key
      this.objectStack.push(this.currentObject);
      this.currentObject = key;
    });

    this.parser.on('openobject',(key) => {
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
            this.currentTable = key; 
            this.push({ table : key});
			this.startTime = performance.now();
			this.rowsRead = 0;
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

    this.parser.on('openarray',() => {
      // this.yadamuLogger.trace([`${this.constructor.name}.onOpenArray()`,`${this.jDepth}`],'ObjectStack: ${this.objectStack}`);
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
      // this.yadamuLogger.trace([`${this.constructor.name}.onCloseObject()`,`${this.jDepth}`],`\nObjectStack: ${this.objectStack}\nCurrentObject: ${this.currentObject}`);           
      this.jDepth--;

      switch (this.jDepth){
		case 0:
          this.push({eof:true});
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
 		      this.push({[key]: this.currentObject});
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
   
    this.parser.on('closearray',() => {
      // this.yadamuLogger.trace([`${this.constructor.name}.onclosearray()`,`${this.jDepth}`],`\nObjectStack: ${this.objectStack}.\nCurrentObject:${this.currentObject}`);          
      this.jDepth--;

      let skipObject = false;

      switch (this.jDepth){
		case 0:
          this.push({eof:true});
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
			this.endTime = performance.now();
			const tableReadStatistics =  {tableName: this.currentTable, rowsRead: this.rowsRead, pipeStartTime: this.startTime, readerEndTime: this.endTime, parserEndTime: this.endTime, copyFailed: false}
		    this.push({eod: tableReadStatistics})
            this.tableList.delete(this.currentTable);
          }
          break;
        case 3:
          if (this.dataPhase) {
            this.push({ data : this.currentObject});
			this.rowsRead++;
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
   
   
    this.tableList  = new Set();
    this.objectStack = [];
    this.dataPhase = false;     
    
    this.currentObject = undefined;
    this.chunks = [];

    this.jDepth = 0; 
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

}

module.exports = JSONParser;
