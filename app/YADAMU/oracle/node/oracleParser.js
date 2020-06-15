"use strict" 

const oracledb = require('oracledb');

const YadamuParser = require('../../common/yadamuParser.js')
const StringWriter = require('../../common/stringWriter.js');
const BufferWriter = require('../../common/bufferWriter.js');

class OracleParser extends YadamuParser {
  
  constructor(tableInfo,objectMode,yadamuLogger) {
    super(tableInfo,objectMode,yadamuLogger);      
    this.columnMetadata = undefined;
    this.includesLobs = false;
  }

  setColumnMetadata(metadata) {
    this.columnMetadata = metadata
    this.columnMetadata.forEach((column) => {
      if ((column.fetchType === oracledb.CLOB) || (column.fetchType === oracledb.BLOB)) {
        this.includesLobs = true;
      }
    })
  }
  
  blob2HexBinary(blob) {
  
    return new Promise(async (resolve,reject) => {
      try {
      const bufferWriter = new  BufferWriter();
          
        blob.on('error',async (err) => {
           await blob.close();
           reject(err);
        });
          
        bufferWriter.on('finish',async () => {
          await blob.close(); 
          resolve(bufferWriter.toHexBinary());
        });
         
        blob.pipe(bufferWriter);
      } catch (err) {
        reject(err);
      }
    });
  };
    
  clob2String(clob) {
     
    return new Promise(async (resolve,reject) => {
      try {
        const stringWriter = new  StringWriter();
        clob.setEncoding('utf8');  // set the encoding so we get a 'string' not a 'buffer'
        
        clob.on('error',async (err) => {
           await clob.close();
           reject(err);
        });
        
        stringWriter.on('finish',async () => {
          await clob.close(); 
          resolve(stringWriter.toString());
        });
       
        clob.pipe(stringWriter);
      } catch (err) {
        reject(err);
      }
    });
  };
  
  async _transform (data,encoding,callback) {
	try {
      this.counter++;
	  if (this.includesLobs) {
        data = await Promise.all(data.map((item,idx) => {
                 if ((item !== null) && (this.columnMetadata[idx].fetchType === oracledb.CLOB)) {
                   return this.clob2String(item)
                 } 
                 if ((item !== null) && (this.columnMetadata[idx].fetchType === oracledb.BLOB)) {
                   return this.blob2HexBinary(item)
                 }  
                 return item
        }))
      }  

      // Convert the JSON columns into JSON objects
      this.tableInfo.jsonColumns.forEach((idx) => {
        if (data[idx] !== null) {
          try {
            data[idx] = JSON.parse(data[idx]) 
          } catch (e) {
            this.yadamuLogger.logException([`${this.constructor.name}._transform()`,`${this.counter}`],e);
            this.yadamuLogger.writeDirect(`${data[idx]}\n`);
          } 
        }
      })
      
      this.tableInfo.rawColumns.forEach((idx) => {
        if (data[idx] !== null) {
          if (Buffer.isBuffer(data[idx])) {
            data[idx] = data[idx].toString('hex');
          }
        }
      })
    
      if (!this.objectMode) {
        data = JSON.stringify(data);
      }
      const res = this.push({data:data})
      callback();
	} catch (e) {
      callback(e)
	}
  }

}

module.exports = OracleParser