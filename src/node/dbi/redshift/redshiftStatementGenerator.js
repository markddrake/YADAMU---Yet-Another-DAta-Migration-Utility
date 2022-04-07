
import path                   from 'path';
import crypto                 from 'crypto';

import YadamuDataTypes        from './yadamuDataTypes.js'

class StatementGenerator {

  constructor(dbi, targetSchema, metadata, yadamuLogger) {
    this.dbi = dbi;
    this.targetSchema = targetSchema
    this.metadata = metadata
    this.yadamuLogger = yadamuLogger;
	
    this.dbi.IAM_ROLE = 'arn:aws:iam::437125103918:role/RedshiftFastLoad'	

  }

  getMappedDataType(dataType,sizeConstraint) {
	  
	  
    const mappedDataType = super.getMappedDataType(dataType,sizeConstraints[idx])
    const length = parseInt(sizeConstraint)

	mappedDataTypes.push(mappedDataType)
	  
    let targetLength = dataType.length
	 
	switch (mappedDataType) {
	  case this.dbi.DATATYPES.VARCHAR_TYPE:
        targetLength = tableMetadata.vendor === 'Redshift' ? targetLength : Math.ceil(targetLength * this.dbi.BYTE_TO_CHAR_RATIO);
	    if (targetLength > StatementGenerator.LARGEST_VARCHAR_SIZE) {
		  targetLength = StatementGenerator.LARGEST_VARCHAR_SIZE
		}
		sizeConstraints[idx] = targetLength
    }		
  }
    
  generateDDLStatement(schema,tableName,columnDefinitions,mappedDataTypes) {
	  
    return `create table if not exists "${schema}"."${tableName}"(\n  ${columnDefinitions.join(',')})`;
	
  }

  generateDMLStatement(schema,tableName,columnNames,insertOperators) {
    return `insert into "${this.targetSchema}"."${tableMetadata.tableName}" ("${columnNames.join('","')}") values `;
  }

  
  generateCopyOperation(schema,tableName,datafile) {  
	return `copy "${schema}"."${tableName}" from 's3://${this.dbi.BUCKET}/${dataFile}' iam_role '${this.dbi.IAM_ROLE}' EMPTYASNULL DATEFORMAT 'auto' TIMEFORMAT 'auto' MAXERROR ${this.dbi.TABLE_MAX_ERRORS} FORMAT AS CSV`		
  }

  generateTableInfo(tableMetadata)
  
    const tableInfo = super.generateTableInfo(tableMetadata)

    tableInfo.maxBatchSize  = Math.trunc(32768 / tableMetadata.columnNames.length);
	if (tableMetadata.dataFile) {
	  tableInfo.copy = generateCopyOperation(this.targetSchema,tableMetadata.tableName,tableMetadata.datafile)
	}
	
	return tableInfo
	
  }
  
}

export { StatementGenerator as default }