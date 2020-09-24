"use strict"

const WKX = require('wkx');

class YadamuSpatialLibrary {
  
  constructor() {
  }

  /*
  **
  ** Convert a Buffer containing WKB to WKT
  **
  */
 
  static bufferToWkT(geometry) {
    return WKX.Geometry.parse(geometry).toWkt()
  }
   
  /*
  **
  ** Convert a String containing a HexBinary representation of WKB to WKT
  **
  */
 
  static hexBinaryToWKT(geometry) {
    return WKX.Geometry.parse(Buffer.from(geometry,'hex')).toWkt()
  }
 
  /*
  **
  ** Convert a Buffer containing WKB to WKT
  **
  */
 
  static bufferToGeoJSON(geometry) {
    return WKX.Geometry.parse(geometry).toGeoJSON()
  }
   
  /*
  **
  ** Convert a String containing a HexBinary representation of WKB to WKT
  **
  */
 
  static hexBinaryToGeoJSON(geometry) {
    return WKX.Geometry.parse(Buffer.from(geometry,'hex')).toGeoJSON()
  }

  static wktToGeoJSON(geometry) {
    return WKX.Geometry.parse(geometry).toGeoJSON()
  }
  
  static geoJSONtoWKT(geometry) {
    return  WKX.Geometry.parseGeoJSON(typeof geometry === "string" ? JSON.parse(geometry) : geometry).toWkt();
  }

  static geoJSONtoEWKT(geometry) {
    return  WKX.Geometry.parseGeoJSON(typeof geometry === "string" ? JSON.parse(geometry) : geometry).toEwkt();
  }

  static geoJSONtoWKB(geometry) {
    return  WKX.Geometry.parseGeoJSON(typeof geometry === "string" ? JSON.parse(geometry) : geometry).toWkb();
  }

  static geoJSONtoEWKB(geometry) {
    return  WKX.Geometry.parseGeoJSON(typeof geometry === "string" ? JSON.parse(geometry) : geometry).toEwkt();
  }
  
  static recodeSpatialColumns(sourceFormat,targetFormat,dataTypes,batch,arrayOfRows) {
    
    /*
    **
    ** Recodes the spatial content of a batch of records.
    **
    ** The batch can be in one of two formats
    **
    **   Array of Rows. Each Element in the Batch represents one row. The element consists of an array of column values.
    **   Array of Columns  The batch consists of a single flat array of values. The first 'n' elements, map to the first row, the next 'n' elements map to the second row.
    **
    */
      
    
    // Find the colunmns to be converted
    const spatialColumnList = []
    dataTypes.forEach((dataType,idx) => {
      switch (dataType.toUpperCase()){
        case 'GEOGRAPHY':
        case 'GEOMETRY':
        case '"MDSYS"."SDO_GEOMETRY"':
          spatialColumnList.push(idx)
          break;
        default:
      }
    })
        
    if (spatialColumnList.length === 0) {
      return
    }  

    // Create an array of conversion functions
    
    const spatialConversions = new Array(dataTypes.length).fill(null)
    spatialColumnList.forEach((spatialIdx) => {
      const testColumn = arrayOfRows ? batch[0][spatialIdx] : batch[spatialIdx]
      switch (true) {
        case ((sourceFormat === 'EWKB') && (targetFormat === 'EWKT') && Buffer.isBuffer(testColumn)):
        case ((sourceFormat === 'EWKB') && (targetFormat === 'WKT') && Buffer.isBuffer(testColumn)):
        case ((sourceFormat === 'WKB') && (targetFormat === 'EWKT') && Buffer.isBuffer(testColumn)):
        case ((sourceFormat === 'WKB') && (targetFormat === 'WKT') && Buffer.isBuffer(testColumn)):
          spatialConversions[spatialIdx] = this.bufferToWkT
          break;
        case ((sourceFormat === 'EWKB') && (targetFormat === 'EWKT')):
        case ((sourceFormat === 'EWKB') && (targetFormat === 'WKT')):
        case ((sourceFormat === 'WKB') && (targetFormat === 'EWKT')):
        case ((sourceFormat === 'WKB') && (targetFormat === 'WKT')):
          spatialConversions[spatialIdx] = YadamuSpatialLibrary.hexBinaryToWKT
          break;      
        case ((sourceFormat === 'EWKB') && (targetFormat === 'GeoJSON') && Buffer.isBuffer(testColumn)):
        case ((sourceFormat === 'WKB') && (targetFormat === 'GeoJSON') && Buffer.isBuffer(testColumn)):
          spatialConversions[spatialIdx] = this.bufferToGeoJSON
          break;
        case ((sourceFormat === 'EWKB') && (targetFormat === 'GeoJSON')):
        case ((sourceFormat === 'WKB') && (targetFormat === 'GeoJSON')):
          spatialConversions[spatialIdx] = this.hexBinaryToGeoJSON
          break;      
        case ((sourceFormat === 'EWKT') && (targetFormat === 'GeoJSON')):
        case ((sourceFormat === 'WKT') && (targetFormat === 'GeoJSON')):
          spatialConversions[spatialIdx] = this.wktToGeoJSON
          break;      
        case ((sourceFormat === 'GeoJSON') && (targetFormat === 'EWKT')):
          spatialConversions[spatialIdx] = this.geoJSONtoEWKT
          break;      
        case ((sourceFormat === 'GeoJSON') && (targetFormat === 'WKT')):
          spatialConversions[spatialIdx] = this.geoJSONtoWKT
          break;      
        case ((sourceFormat === 'GeoJSON') && (targetFormat === 'EWKB')):
          spatialConversions[spatialIdx] = this.geoJSONtoEWKB
          break;      
        case ((sourceFormat === 'GeoJSON') && (targetFormat === 'WKB')):
          spatialConversions[spatialIdx] = this.geoJSONtoWKB
          break;      
       default:
      }
    })
    
    if (spatialConversions.every((currentValue) => { currentValue === null})) {
      return
    }
    
    if (arrayOfRows) {
      batch.forEach((row) => {
        spatialConversions.forEach((spatialConversion,idx) => {
          if ((spatialConversion != null) && (row[idx] != null)) {
            row[idx] = spatialConversion(row[idx]);
          }
        })
      })
    }
    else {
      const columnList = [...spatialColumnList]
      while (columnList[0] < batch.length) {
        columnList.forEach((columnIdx,idx) => {
          if ((spatialConversions[spatialColumnList[idx]] != null) && (batch[columnIdx] != null)) {
            const spatialConversion = spatialConversions[spatialColumnList[idx]]
            batch[columnIdx] = spatialConversion(batch[columnIdx])
          }
          columnList[idx] = columnList[idx] + dataTypes.length
        })
      }
    }
  }
}
  
module.exports = YadamuSpatialLibrary