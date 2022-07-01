class CSVLibrary {

  static getCSVTransformation(col,isLastColumn,dataType) {

    switch (typeof col) {
	  case "number":
        return (isLastColumn)
 	    ? (os,col) => {
   		    os.write(col.toString());
			os.write('\n');
		  } 
		: (os,col) => {
   		    os.write(col.toString());
			os.write(',');
		  }
		case "boolean":
		  return (isLastColumn) 
		  ? (os,col) => {
  		      os.write(col ? 'true' : 'false')
			  os.write('\n');
		    } 
		  : (os,col) => {
  		      os.write(col ? 'true' : 'false')
			  os.write(',');
		    } 
		case "string":
		  return (isLastColumn)
		  ? (os,col) => {
		      // sw.write(JSON.stringify(col));
              os.write('"')
	  	      os.write(col.indexOf('"') > -1 ? col.replace(/"/g,'""') : col)
		      os.write('"\n')
		    } 
		  : (os,col) => {
		      // sw.write(JSON.stringify(col));
              os.write('"')
	  	      os.write(col.indexOf('"') > -1 ? col.replace(/"/g,'""') : col)
		      os.write('",')
		    } 
	    case "object":
		  switch (true) {
		    case (col instanceof Date):
		      return (isLastColumn)
		      ? (os,col) => {
                  os.write('"')
                  os.write(col === null ? '' : col.toISOString())
		          os.write('"\n')
		        } 
		      : (os,col) => {
                os.write('"')
		        os.write(col === null ? '' : col.toISOString())
		        os.write('",')
		      } 
		    case (Buffer.isBuffer(col)):
		      return (isLastColumn)
		      ? (os,col) => {
                  os.write('"')
                  os.write(col === null ? '' : col.toString('hex'))
		          os.write('"\n')
		        } 
		      : (os,col) => {
                os.write('"')
		        os.write(col === null ? '' : col.toString('hex'))
		        os.write('",')
		      } 
			default:
		      return (isLastColumn)
		      ? (os,col) => {
                  os.write('"')
		          os.write(col === null ? '' : JSON.stringify(col).replace(/"/g,'""'))
		          os.write('"\n')
		        } 
		      : (os,col) => {
                  os.write('"')
		          os.write(col === null ? '' : JSON.stringify(col).replace(/"/g,'""'))
		          os.write('",')
		        } 
		  }
		default:
		  return (isLastColumn)
		  ? (os,col) => {
  		      os.write(col);
			  os.write('\n');
		    } 
	      : (os,col) => {
  		      os.write(col);
		  	  os.write(',');
		    } 
	}
  }	 
  
  static getCSVTransformations(batch,dataTypes) {

    // RFC4180

    // Set the CSV Transformation functions based on the first non-null value for each column.

	const lastIdx = batch[0].length - 1
	return batch[0].map((col,colIdx) => {
       const lastColumn = colIdx === lastIdx
	   return col !== null ? this.getCSVTransformation(col,lastColumn,dataTypes[colIdx].toLowerCase()) : this.getCSVTransformation(batch[batch.findIndex((row) => {return row[colIdx] !== null})]?.[colIdx],lastColumn,dataTypes[colIdx].toLowerCase())
	})
  }

  static writeRowAsCSV(os,row,transformations) {
	row.forEach((col,idx) => {
	  if (col === null) {
		os.write((idx < (row.length-1)) ? ',' : '\n')
	  }
	  else {
        transformations[idx](os,col)
	  }
	})
  }
  
  static writeBatchAsCSV(os,batch,transformations) {  
    batch.forEach((row) => {
	   this.writeRowAsCSV(os,row,transformations)
    })
  }     
}

export { CSVLibrary as default}