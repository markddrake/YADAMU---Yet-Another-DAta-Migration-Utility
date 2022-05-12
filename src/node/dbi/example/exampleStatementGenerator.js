
import YadamuStatementGenerator  from '../base/yadamuStatementGenerator.js'

class ExampleStatementGenerator extends YadamuStatementGenerator {
  
  constructor(dbi, vendor, targetSchema, metadata, yadamuLogger) {
    super(dbi, vendor, targetSchema, metadata, yadamuLogger)
  }

}

export { ExampleStatementGenerator as default }