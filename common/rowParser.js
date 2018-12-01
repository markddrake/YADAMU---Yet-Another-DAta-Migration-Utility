"use strict" 

const { Transform } = require('stream');
const Readable = require('stream').Readable;
// const clarinet = require('clarinet');
const clarinet = require('../clarinet/clarinet.js');


class RowParser extends Transform {
  
  constructor(logWriter, options) {

    super({objectMode: true });  
  
    const self = this;
    
    this.logWriter = logWriter;

    this.parser = clarinet.createStream();
    
    this.parser.on('error',
    function(err) {
      self.logWriter.write(`${err}\n`);
    })
    
    this.parser.on('key',
    function (key) {
      // self.logWriter.write(`onKey(${self.jDepth},${key})\n`);
      
      switch (self.jDepth){
        case 1:
          // Push the completed first level object/array downstream. Replace the current top level object with an empty object of the same type.
          self.push(self.currentObject);
          if (self.currentObject.metadata) {
            self.tableList = new Set(Object.keys(self.currentObject.metadata));
          }
          if (Array.isArray(self.currentObject)) {
             self.currentObject = [];
          }
          else {
             self.currentObject = {};
          }
          if (key === 'data') {
            self.dataPhase = true;
          }
          break;
        case 2:
          if (self.dataPhase) {
            self.tableList.delete(key);
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
      // self.logWriter.write(`onOpenObject(${self.jDepth}:, Key:"${key}". ObjectStack:${self.objectStack}\n`);      
      
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
          if (key === 'data') {
            self.dataPhase = true;
          }
          break;
        case 1:
          if ((self.dataPhase) && (key != undefined)) {
            self.tableList.delete(key);
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
      // self.logWriter.write(`onOpenArray(${self.jDepth}): ObjectStack:${self.objectStack}\n`);
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
      // self.logWriter.write(`onvalue(${self.jDepth}: ObjectStack:${self.objectStack}\n`);        
      if (self.chunks.length > 0) {
        self.chunks.push(v);
        v = self.chunks.join('');
        self.chunks = []
      }
      
      if (typeof v === 'boolean') {
        v = new Boolean(v).toString();
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
      // self.logWriter.write(`onCloseObject(${self.jDepth}):\nObjectStack:${self.objectStack})\nCurrentObject:${self.currentObject}\n`);           
      self.jDepth--;

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
    });
   
    this.parser.on('closearray',
    function () {
      // self.logWriter.write(`onclosearray(${self.jDepth}: ObjectStack:${self.objectStack}. CurrentObject:${self.currentObject}\n`);          
      let skipObject = false;
      
      if ((self.dataPhase) && (self.jDepth === 4)) {
        self.push({ data : self.currentObject});
        skipObject = true;
      }

      self.jDepth--;

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
   
   
    this.tableList  = undefined;
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
        this.logWriter.write(`${new Date().toISOString()}[WARNING]: Table "${table}". No records found - Possible corrupt or truncated import file.\n`);
      },this)
      return true;
    }
  };
   
  _transform(data,enc,callback) {
    this.parser.write(data);
    callback();
  };
}

module.exports = RowParser;
