
import YadamuParser from '../base/yadamuParser.js'

class ExampleParser extends YadamuParser {
  
  constructor(dbi,queryInfo,pipelineState,yadamuLogger) {
    super(dbi,queryInfo,pipelineState,yadamuLogger)    	
  }
  
  /*
  **
  ** For Databases that return objects rather than arrays
  ** 
  
  async doTransform(data) {
    data = Object.values(data)    
	return await super.doTransform(data)
  }
  
  **
  */
  
}

export { ExampleParser as default}