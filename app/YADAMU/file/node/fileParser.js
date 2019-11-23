"use strict" 

const { Transform } = require('stream');
const Readable = require('stream').Readable;
// const clarinet = require('clarinet');
const clarinet = require('../../clarinet/clarinet.js');

class TextParser extends Transform {
  
  constructor(yadamuLogger, options) {

    super({objectMode: true });  
  
    const self = this;
    
    this.yadamuLogger = yadamuLogger;

    this.parser = clarinet.createStream();
    
    this.parser.on('error',
    function(err) {
      yadamuLogger.logException([`${this.constructor.name}.onError()`],err)
    })
    
    this.parser.on('key',
    function (key) {
      // self.yadamuLogger.trace([`${self.constructor.this}.onKey()`,`${self.jDepth}`,`"${key}"`],``);
      
      switch (self.jDepth){
        case 1:
          if (key === 'data') {
            self.dataPhase = true;
          }
          break;
        case 2:
          if (self.dataPhase) {
            self.currentTable = key;                
            self.push({ table : key});
          }
          break;
        default:
      }
      // Push the current object onto the stack and the current object to the key
      self.objectStack.push(self.currentObject);
      self.currentObject = key;
    });

    this.parser.on('openobject',
    function (key) {
      // self.yadamuLogger.trace([`${self.constructor.this}.onOpenObject()`,`${self.jDepth}`,`"${key}"`],`ObjectStack:${self.objectStack}\n`);      
      
      if (self.jDepth > 0) {
        self.objectStack.push(self.currentObject);
      }
         
      switch (self.jDepth) {
        case 0:
          // Push the completed first level object/array downstream. Replace the current top level object with an empty object of the same type.
          if (self.currentObject !== undefined) {
            if (self.currentObject.metadata) {
              self.tableList = new Set(Object.keys(self.currentObject.metadata));
            }
            self.push(self.currentObject);
          }  
          break;
        case 1:
          if ((self.dataPhase) && (key != undefined)) {
            self.currentTable = key;                
            self.push({ table : key});
          }
          break;
        default:
      }
      // If the object has a key put the object on the stack and set the current object to the key. 
      self.currentObject = {}
      self.jDepth++;
      if (key !== undefined) {
        self.objectStack.push(self.currentObject);
        self.currentObject = key;
      }
    });

    this.parser.on('openarray',
    function () {
      // self.yadamuLogger.trace([`${self.constructor.this}.onOpenArray()`,`${self.jDepth}`],'ObjectStack: ${self.objectStack}`);
      if (self.jDepth > 0) {
        self.objectStack.push(self.currentObject);
      }
      self.currentObject = [];
      self.jDepth++;
    });


    this.parser.on('valuechunk',
    function (v) {
      self.chunks.push(v);  
    });
       
    this.parser.on('value',
    function (v) {
      // self.yadamuLogger.trace([`${self.constructor.this}.onvalue()`,`${self.jDepth}`],`ObjectStack: ${self.objectStack}\n`);        
      if (self.chunks.length > 0) {
        self.chunks.push(v);
        v = self.chunks.join('');
        self.chunks = []
      }
      
      if (Array.isArray(self.currentObject)) {
          // currentObject is an ARRAY. We got a value so add it to the Array
          self.currentObject.push(v);
      }
      else {
          // currentObject is an Key. We got a value so fetch the parent object and add the KEY:VALUE pair to it. Parent Object becomes the Current Object.
          const parentObject = self.objectStack.pop();
          parentObject[self.currentObject] = v;
          self.currentObject = parentObject;
      }
    });
      
    this.parser.on('closeobject',
    async function () {
      // self.yadamuLogger.trace([`${self.constructor.this}.onCloseObject()`,`${self.jDepth}`],`\nObjectStack: ${self.objectStack}\nCurrentObject: ${self.currentObject}`);           
      self.jDepth--;

      switch (self.jDepth){
        case 1:
          // Push the completed first level object/array downstream. Replace the current top level object with an empty object of the same type.
          if (self.currentObject.metadata) {
            self.tableList = new Set(Object.keys(self.currentObject.metadata));
          }
          const key = self.objectStack.pop()
          self.objectStack.pop()
          if (self.dataPhase) {
            self.dataPhase = false;
          }
          else {
            self.push({[key]: self.currentObject});
          }
          self.currentObject = {};
          break;
        default:
          // An object can belong to an Array or a Key
          if (self.objectStack.length > 0) {
            let owner = self.objectStack.pop()
            let parentObject = undefined;
            if (Array.isArray(owner)) {   
              parentObject = owner;
              parentObject.push(self.currentObject);
            }    
            else {
              parentObject = self.objectStack.pop()
              if (!this.emptyObject) {
                parentObject[owner] = self.currentObject;
              }
            }   
            self.currentObject = parentObject;
          }
      }
    });
   
    this.parser.on('closearray',
    function () {
      // self.yadamuLogger.trace([`${self.constructor.this}.onclosearray()`,`${self.jDepth}`],\nObjectStack: ${self.objectStack}.\nCurrentObject:${self.currentObject}`);          
      self.jDepth--;

      let skipObject = false;

      switch (self.jDepth){
        case 1:
          // Push the completed first level object/array downstream. Replace the current top level object with an empty object of the same type.
          self.push(self.currentObject);
          self.currentObject = [];
          break;
        case 2:
          if (self.dataPhase) {
            self.tableList.delete(self.currentTable);
          }
          break;
        case 3:
          if (self.dataPhase) {
            self.push({ data : self.currentObject});
            skipObject = true;
          }
      }

      // An Array can belong to an Array or a Key
      if (self.objectStack.length > 0) {
        let owner = self.objectStack.pop()
        let parentObject = undefined;
        if (Array.isArray(owner)) {   
          parentObject = owner;
          if (!skipObject) {
            parentObject.push(self.currentObject);
          }
        }    
        else {
          parentObject = self.objectStack.pop()
          if (!skipObject) {
            parentObject[owner] = self.currentObject;
          }
        }
        self.currentObject = parentObject;
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
      this.tableList.forEach(function(table) {
        this.yadamuLogger.warning([`${this.constructor.name}`,`"${table}"`],`No records found - Possible corrupt or truncated import file.\n`);
      },this)
      return true;
    }
  };
   
  _transform(data,enc,callback) {
    this.parser.write(data);
    callback();
  };
}

module.exports = TextParser;
