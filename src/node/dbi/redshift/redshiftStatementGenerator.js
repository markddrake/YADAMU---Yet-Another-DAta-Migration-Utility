
import path                      from 'path';
import crypto                    from 'crypto';

import YadamuDataTypes           from '../base/yadamuDataTypes.js'
import YadamuStatementGenerator  from '../base/yadamuStatementGenerator.js'


class RedshiftStatementGenerator extends YadamuStatementGenerator {

  constructor(dbi, vendor, targetSchema, metadata, yadamuLogger) {
    super(dbi, vendor, targetSchema, metadata, yadamuLogger)
    this.dbi.IAM_ROLE = 'arn:aws:iam::437125103918:role/RedshiftFastLoad'	
  }

  generateDDLStatement(schema,tableName,columnDefinitions,mappedDataTypes) {
	  
    return `create table if not exists "${schema}"."${tableName}"(\n  ${columnDefinitions.join(',')})`;
	
  }

  generateDMLStatement(schema,tableName,columnNames,insertOperators) {
    return `insert into "${schema}"."${tableName}" ("${columnNames.join('","')}") values `;
  }

  
  generateCopyOperation(schema,tableName,datafile) {  
	return `copy "${schema}"."${tableName}" from 's3://${this.dbi.BUCKET}/${datafile}' iam_role '${this.dbi.IAM_ROLE}' EMPTYASNULL DATEFORMAT 'auto' TIMEFORMAT 'auto' MAXERROR ${this.dbi.TABLE_MAX_ERRORS} FORMAT AS CSV`		
  }

  generateTableInfo(tableMetadata) {

    const tableInfo = super.generateTableInfo(tableMetadata)

    tableInfo.maxBatchSize  = Math.trunc(32768 / tableMetadata.columnNames.length);

	if (tableMetadata.dataFile) {
	  tableInfo.copy = {
         dml: this.generateCopyOperation(this.targetSchema,tableMetadata.tableName,tableMetadata.datafile)
      }
	}
	
	return tableInfo
	
  }
  
}

export { RedshiftStatementGenerator as default }