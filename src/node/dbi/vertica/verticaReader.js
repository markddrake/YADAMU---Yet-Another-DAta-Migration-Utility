
import {Readable} from 'stream';
import pg from 'pg';
const {Query,Pool} = pg;
import types from 'pg-types';

import array from 'postgres-array'
import parseByteA from 'postgres-bytea'

/*
**
** Vertica TypeID's from Vertica 10.0.1

----------------------------------+-------------------
 Array[Binary]                    |              1522
 Array[Boolean]                   |              1505
 Array[Char]                      |              1508
 Array[Date]                      |              1510
 Array[Float8]                    |              1507
 Array[Int8]                      |              1506
 Array[Interval Day to Hour]      |              1514
 Array[Interval Day to Minute]    |              1514
 Array[Interval Day to Second]    |              1514
 Array[Interval Day]              |              1514
 Array[Interval Hour to Minute]   |              1514
 Array[Interval Hour to Second]   |              1514
 Array[Interval Hour]             |              1514
 Array[Interval Minute to Second] |              1514
 Array[Interval Minute]           |              1514
 Array[Interval Month]            |              1521
 Array[Interval Second]           |              1514
 Array[Interval Year to Month]    |              1521
 Array[Interval Year]             |              1521
 Array[Numeric]                   |              1516
 Array[TimeTz]                    |              1515
 Array[Time]                      |              1511
 Array[TimestampTz]               |              1513
 Array[Timestamp]                 |              1512
 Array[Uuid]                      |              1520
 Array[Varbinary]                 |              1517
 Array[Varchar]                   |              1509
 Binary                           |               117
 Boolean                          |                 5
 Char                             |                 8
 Date                             |                10
 Float                            |                 7
 Integer                          |                 6
 Interval Day                     |                14
 Interval Day to Hour             |                14
 Interval Day to Minute           |                14
 Interval Day to Second           |                14
 Interval Hour                    |                14
 Interval Hour to Minute          |                14
 Interval Hour to Second          |                14
 Interval Minute                  |                14
 Interval Minute to Second        |                14
 Interval Month                   |               114
 Interval Second                  |                14
 Interval Year                    |               114
 Interval Year to Month           |               114
 Long Varbinary                   |               116
 Long Varchar                     |               115
 Numeric                          |                16
 Set[Binary]                      |              2722
 Set[Boolean]                     |              2705
 Set[Char]                        |              2708
 Set[Date]                        |              2710
 Set[Float8]                      |              2707
 Set[Int8]                        |              2706
 Set[Interval Day to Hour]        |              2714
 Set[Interval Day to Minute]      |              2714
 Set[Interval Day to Second]      |              2714
 Set[Interval Day]                |              2714
 Set[Interval Hour to Minute]     |              2714
 Set[Interval Hour to Second]     |              2714
 Set[Interval Hour]               |              2714
 Set[Interval Minute to Second]   |              2714
 Set[Interval Minute]             |              2714
 Set[Interval Month]              |              2721
 Set[Interval Second]             |              2714
 Set[Interval Year to Month]      |              2721
 Set[Interval Year]               |              2721
 Set[Numeric]                     |              2716
 Set[TimeTz]                      |              2715
 Set[Time]                        |              2711
 Set[TimestampTz]                 |              2713
 Set[Timestamp]                   |              2712
 Set[Uuid]                        |              2720
 Set[Varbinary]                   |              2717
 Set[Varchar]                     |              2709
 Time                             |                11
 TimeTz                           |                15
 Timestamp                        |                12
 TimestampTz                      |                13
 Uuid                             |                20
 Varbinary                        |                17
 Varchar                          |                 9
 geography                        | 45035996273705110
 geometry                         | 45035996273705108
(85 rows)

**
*/


class VerticaReader extends Readable {

    get LOGGER()             { return this._LOGGER }
    set LOGGER(v)            { this._LOGGER = v }
  
    get DEBUGGER()           { return this._DEBUGGER }
    set DEBUGGER(v)          { this._DEBUGGER = v }
      
    constructor(connection,sqlStatement,tableName,yadamuLogger) {
	  super({objectMode:true}) 
	  this.connection = connection
	  this.tableName = tableName

      this.LOGGER = yadamuLogger
	  
	  this.stagingArea = []
      this.highWaterMark = 1024
	  this.lowWaterMark = 512
	  this.streamComplete = false;
	  this.streamPaused = false;
	  this.streamFailed = false;
      this.pendingRead = false
	  let count = 0;
	  	  
      types.setTypeParser(116,parseByteA)
	  types.setTypeParser(117,parseByteA)

	  types.setTypeParser(16,this.parseNumeric)
	  types.setTypeParser(5,this.parseBoolean)
	  // types.setTypeParser(14,this.parseIntervalDayToSecond)
	  
	  this.query = new Query({
		text: sqlStatement, 
		rowMode: 'array'
	  });
	  
	  const maxCached = new Promise((resolve,reject) => {
		let maxCached = 0;
	    this.query.on('row',(row) => {
	      if (this.pendingRead) { 
	        const res = this.push(row)
		    this.pendingRead = false
		  }
		  else {
		    this.stagingArea.push(Object.values(row));
		    maxCached = this.stagingArea.length > maxCached ? this.stagingArea.length : maxCached;
		    count++
		    
			if (this.stagingArea.length === this.highWaterMark) {
			  this.connection.connection.stream.pause();
			  this.streamPaused = true;
		    }
			
		  }
		}).on('end',() => {
  	      this.streamComplete = true;
	      if (this.pendingRead) {
	        this.push(null)
		  }
		  resolve(maxCached)
		}).on('error',(err) => {
   	      this.streamFailed = true;
		  this.streamComplete = true;
		  this.emit('error',err)
		  this.destroy(err);
		})
      })
      const result = this.connection.query(this.query)	  
	}
	
	parseNumeric = (value) => {
	  return value
	}
	
	parseBoolean = (value) => {
	  switch (value) {
	    case 't' :
		  return true;
		case 'f':
		  return false;
		default:
          return value
      }
	}
	
	parseIntervalDayToSecond = (value) => {
       return value
	}

	_read() {
      if (this.stagingArea.length > 0) {
        const res  = this.push(this.stagingArea.shift())
	    this.pendingRead = false
		if (this.streamPaused && (this.stagingArea.length === this.lowWaterMark)) {
		  this.connection.connection.stream.resume();
		  this.streamPaused  = false;
	    }
	  }
	  else {
	    if (this.streamComplete) {
	      this.push(null)
		}
		else {
          this.pendingRead = true;		
		}
	  }
	}
	
	/*
	async _destroy(cause,callback) {
       // this.LOGGER.trace([this.constructor.name,this.tableName],`_destroy(${cause ? cause.message : 'Normal'})`)
	   if (!this.streamComplete) {
		 try {
		   await this.request.cancel();
		   this.streamCancelled = true;
		 } catch (e) {
	     }
	   }
	   callback(cause)
	}
	*/
	
}

export { VerticaReader as default }