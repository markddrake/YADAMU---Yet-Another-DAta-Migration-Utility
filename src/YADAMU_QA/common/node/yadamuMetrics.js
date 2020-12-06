
class YadamuMetrics {

  constructor() {
  
    this.suite        = this.initializeMetrics()    
    this.test         = this.initializeMetrics()    
	this.target       = this.initializeMetrics()    
	this.task         = this.initializeMetrics()  
    this.subTask 	  = this.initializeMetrics()  
    this.timings = []
  }
 
  initializeMetrics() {
    return {
	  errors    : 0
	, warnings  : 0
	, failed    : 0
	}
  }
  
  aggregateMetrics(cummulative, metrics, retainMetrics) {
	cummulative.errors+= metrics.errors;
	cummulative.warnings+= metrics.warnings;
	cummulative.failed+= metrics.failed;
	if (!retainMetrics) this.resetMetrics(metrics)
  }
  
  newTarget() {
	this.target = this.initializeMetrics()
  }
  
  aggregateSuite() {
	 this.aggregateMetrics(this.suite,this.test)
  }
  
  aggregateTest() {
	 this.aggregateMetrics(this.test,this.target)
  }
  
  aggregateTarget() {
	 this.aggregateMetrics(this.target,this.task)
  }
  
  aggregateTask() {
	 this.aggregateMetrics(this.task,this.subTask)
  }
  
  aggregateSubTask(stepMetrics) {
	 this.aggregateMetrics(this.subTask,stepMetrics)
  }

  resetMetrics(metrics) {
	// Preserve the object by using Object.assign to copy zeroed properties from an new metrics instance.
    Object.assign(metrics,this.initializeMetrics());
  }
	  
  newTest() {
	this.resetMetrics(this.test)
  }

  newTarget() {
	this.resetMetrics(this.target)
  }

  newTask() {
	this.resetMetrics(this.task)
  }

  newSubTask() {
	this.resetMetrics(this.subTask)
	this.timings = []
  }
  
  recordError() {
	this.task.errors++
  }

  recordFailed(failed) {
	this.task.failed += failed
  }

  adjust(metrics) {
	  
	/*
    ** If the copy operations generated errors or warnings the summary message generated at the end of the operation causes the metrics maintained by logger to be incremented by 1.
	** Adjust the metrics obtained from the logger to account for this.
	**
	*/
	
	if (metrics.errors > 1) {
	  metrics.errors--
	}
    else {
	  if (metrics.warnings > 1) {
		metrics.warnings--
	  }
	}
	return metrics;
  }
    
  formatMetrics(metrics) {
	return `Errors: ${metrics.errors}. Warnings: ${metrics.warnings}. Failed: ${metrics.failed}.`
  }
 
  recordTaskTimings(timing) {
    this.timings.push(timing)
  }

}

module.exports = YadamuMetrics