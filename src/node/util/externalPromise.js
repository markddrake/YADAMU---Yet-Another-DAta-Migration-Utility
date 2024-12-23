class ExternalPromise extends Promise {

  constructor(callback) {
	 
	let cache
    super((resolve,reject) => {
	  cache = {resolve,reject}
	  callback(resolve,reject)
	})  
	Object.assign(this,cache)
  }
  
  resolve(x) {
    this.resolve(x)
  }
  
  reject(x) {
	this.reject(x)
  }

}

export {ExternalPromise as default}