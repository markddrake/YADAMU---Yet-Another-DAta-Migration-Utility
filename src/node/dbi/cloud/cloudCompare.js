
import LoaderCompare    from '../loader/loaderCompare.js'

class CloudCompare extends LoaderCompare {

  constructor(dbi,configuration) {
    super(dbi,configuration)
  }

  async compareFiles(sourceFile,targetFile) {
	let props = await this.dbi.cloudService.getObjectProps(sourceFile)
    const sourceFileSize = this.dbi.getContentLength(props)
    props = await this.dbi.cloudService.getObjectProps(targetFile)
    const targetFileSize = this.dbi.getContentLength(props)
    let sourceHash = ''
    let targetHash = ''
    if (sourceFileSize === targetFileSize) {
      sourceHash = await this.calculateHash(sourceFile)
      targetHash = await this.calculateHash(targetFile)
      if (sourceHash !== targetHash) {
        sourceHash = await this.calculateSortedHash(sourceFile);
        targetHash = await this.calculateSortedHash(targetFile)
      }
    }
    return [sourceFileSize,targetFileSize,sourceHash,targetHash]
  }
    
}


export { CloudCompare as default }

