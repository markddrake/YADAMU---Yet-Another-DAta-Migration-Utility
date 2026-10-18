import YadamuParser from '../base/yadamuParser.js'

class SqliteParser extends YadamuParser {

  constructor(dbi, parseJSON, yadamuLogger) {
    super(dbi, parseJSON, yadamuLogger)
  }

  async doParse() {
    await super.doParse()
  }
}

export { SqliteParser as default }
