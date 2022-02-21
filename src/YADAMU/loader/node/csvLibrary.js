class CSVLibrary {

  static getCSVTransformation(col,isLastColumn) {

    switch (typeof col) {
	  case "number":
        return (isLastColumn)
 	    ? (fs,col) => {
   		    fs.write(col.toString());
			fs.write('\n');
		  } 
		: (fs,col) => {
   		    fs.write(col.toString());
			fs.write(',');
		  }
		case "boolean":
		  return (isLastColumn) 
		  ? (fs,col) => {
  		      fs.write(col ? 'true' : 'false')
			  fs.write('\n');
		    } 
		  : (fs,col) => {
  		      fs.write(col ? 'true' : 'false')
			  fs.write(',');
		    } 
		case "string":
		  return (isLastColumn)
		  ? (fs,col) => {
		      // sw.write(JSON.stringify(col));
              fs.write('"')
	  	      fs.write(col.indexOf('"') > -1 ? col.replace(/"/g,'""') : col)
		      fs.write('"\n')
		    } 
		  : (fs,col) => {
		      // sw.write(JSON.stringify(col));
              fs.write('"')
	  	      fs.write(col.indexOf('"') > -1 ? col.replace(/"/g,'""') : col)
		      fs.write('",')
		    } 
	    case "object":
		  switch (true) {
		    case (col instanceof Date):
		      return (isLastColumn)
		      ? (fs,col) => {
                  fs.write('"')
                  fs.write(col === null ? '' : col.toISOString())
		          fs.write('"\n')
		        } 
		      : (fs,col) => {
                fs.write('"')
		        fs.write(col === null ? '' : col.toISOString())
		        fs.write('",')
		      } 
		    case (Buffer.isBuffer(col)):
		      return (isLastColumn)
		      ? (fs,col) => {
                  fs.write('"')
                  fs.write(col === null ? '' : col.toString('hex'))
		          fs.write('"\n')
		        } 
		      : (fs,col) => {
                fs.write('"')
		        fs.write(col === null ? '' : col.toString('hex'))
		        fs.write('",')
		      } 
			default:
		      return (isLastColumn)
		      ? (fs,col) => {
                  fs.write('"')
		          fs.write(col === null ? '' : JSON.stringify(col).replace(/"/g,'""'))
		          fs.write('"\n')
		        } 
		      : (fs,col) => {
                  fs.write('"')
		          fs.write(col === null ? '' : JSON.stringify(col).replace(/"/g,'""'))
		          fs.write('",')
		        } 
		  }
		default:
		  return (isLastColumn)
		  ? (fs,col) => {
  		      fs.write(col);
			  fs.write('\n');
		    } 
	      : (fs,col) => {
  		      fs.write(col);
		  	  fs.write(',');
		    } 
	}
  }	 
  
  static getCSVTransformations(batch) {

    // RFC4180

    // Set the CSV Transformation functions based on the first non-null value for each column.

	const lastIdx = batch[0].length - 1
	return batch[0].map((col,colIdx) => {
       const lastColumn = colIdx === lastIdx
	   return col !== null ? this.getCSVTransformation(col,lastColumn) : this.getCSVTransformation(batch[batch.findIndex((row) => {return row[colIdx] !== null})]?.[colIdx],lastColumn)
	})
  }

  static writeRowAsCSV(fs,row,transformations) {
	row.forEach((col,idx) => {
	  if (col === null) {
		fs.write((idx < (row.length-1)) ? ',' : '\n')
	  }
	  else {
        transformations[idx](fs,col)
	  }
	})
  }
  
  static writeBatchAsCSV(fs,batch,transformations) {  
    batch.forEach((row) => {
	   this.writeRowAsCSV(fs,row,transformations)
    })
  }     
}

export { CSVLibrary as default}