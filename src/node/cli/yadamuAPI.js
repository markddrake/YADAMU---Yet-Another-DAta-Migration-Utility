

import YadamuCLI          from './yadamuCLI.js'

class YadamuAPI extends YadamuCLI {
	
  constructor(configPath) {
	 super()
	 this.yadamu.updateParameters({CONFIG : configPath})
	 this.CONFIGURATION = this.loadConfigurationFile()
  }
	
}

export { YadamuAPI as default}