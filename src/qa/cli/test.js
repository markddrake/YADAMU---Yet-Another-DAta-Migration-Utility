
import YadamuCLI       from '../../node/cli/yadamuCLI.js'
import YadamuLibrary   from '../../node/lib/yadamuLibrary.js'
import Yadamu          from '../core/yadamu.js'

class Test extends YadamuCLI {
  
  createYadamu() {	  
	return new Yadamu(this.command);
  }

  expandConfiguration(configuration,parentFile) {
	super.expandConfiguration(configuration,parentFile)
	if (this.parameters.hasOwnProperty('CONNECTION')) {
	  configuration.connections = YadamuLibrary.loadIncludeFile(this.parameters.CONNECTION,parentFile,this.yadamuLogger)
	}
  }

  setParameter(parameterName,parameterValue) {
	  
    super.setParameter(parameterName,parameterValue)

    switch (parameterName.toUpperCase()) {
      case 'CONN':		  
      case '--CONN':
      case 'CONNECTION':		  
      case '--CONNECTION':
        this.parameters.CONNECTION = parameterValue;
        break;   
   	}
	
  }
}

  async function main() {
 
  try {
    const yadamuTest = new Test();
    try {
      await yadamuTest.doTests();
    } catch (e) {
	  yadamuTest.reportError(e)
    }
    await yadamuTest.close();
  } catch (e) {
	YadamuLibrary.reportError(e)
  }

}

main()

export { Test as default}