"use strict"

const WKX = require('wkx');

const Yadamu = require('../../common/yadamu.js');
const YadamuWriter = require('../../common/yadamuWriter.js');

class TableWriter extends YadamuWriter {

  /*
  **
  ** MongoDB Support allows data from a relational database to be imported using one of the following
  **
  ** If the relational table consists of a single column of Type JSON 
  **
  **   DOCUMENT_MODE:
  **  
  **     If column contains an object then the JSON object will become the mongo document.
  **
  **     If the column contains an array the array will be wrapped in an object containing a single key "array"
  **
  ** If the relational table has multiple columns then three modes are avaialbe 
  ** 
  **    OBJECT_MODE: [Default]: The row is inserted an object. The document contains one key for each column in the table
  **
  **    ARRAY_MODE: The row is inserted as object. The array containing the values from the table will be wrapped in an object containing a single key "row". 
  **                A document containing the table's metadata will be inserted into the YadamuMetadata collection.
  **
  **    BSON_MODE: A BOSN object is contructed based on the relational metadata. 
  **  
  */

  constructor(dbi,tableName,tableInfo,status,yadamuLogger) {
    super(dbi,tableName,tableInfo,status,yadamuLogger)
    
    if ((this.tableInfo.dataTypes.length === 1) && (this.tableInfo.dataTypes[0] === 'JSON')) {
      this.insertMode = 'DOCUMENT_MODE'
    }
    else {
      this.insertMode = this.tableInfo.mongoInsertMode ? this.tableInfo.mongoInsertMode : 'OBJECT_MODE'
    }    
  }

  async initialize() {
      
    this.collection = await this.dbi.getCollection(this.tableName)
    if (this.insertMode === 'ARRAY_MODE') {
      const result = await this.dbi.insertOne('yadamuMetadata',this.tableInfo);
    }
    await this.dbi.beginTransaction();
  }

  async appendRow(row) {
      
    // Convert to GeoJSON
    
    this.tableInfo.dataTypes.forEach(function(sourceDataType,idx) {
      const dataType = this.dbi.decomposeDataType(sourceDataType);
      if (row[idx] !== null) {
        switch (dataType.type) {
          case '"MDSYS"."SDO_GEOMETRY"':
          case 'geography':
          case 'geometry':
            switch (this.dbi.systemInformation.spatialFormat) {
              case "WKB":
                row[idx]  = JSON.stringify(WKX.Geometry.parse(Buffer.from(row[idx],'hex')).toGeoJSON())
                break;
              case "EWKB":
                break;
              case "WKT":
                break;
              case "EWKT":
                break;
              default:
            }
            break;
          default:
        }
      }
    },this)

    switch (this.insertMode) {
      case 'DOCUMENT_MODE' :
        if (Array.isArray(row[0])) {
          this.batch.push({array : row[0]})
        }
        else {
          this.batch.push(row[0]);
        }
        break;
      case 'BSON_MODE':
      case 'OBJECT_MODE' :
        const mDocument = {}
        this.tableInfo.keys.forEach(function(key,idx) {
           mDocument[key] = row[idx]
        },this);
        this.batch.push(mDocument);
        break;
      case 'ARRAY_MODE' :
        this.batch.push({ row : row });
        break;
      default:
        // ### Exception - Unknown Mode
    }
    return this.batch.length;

  }

  async writeBatch() {
      
    this.batchCount++;
    await this.dbi.insertMany(this.tableName,this.batch);
    this.endTime = new Date().getTime();
    this.batch.length = 0;  
    return this.skipTable
  }
}

module.exports = TableWriter;