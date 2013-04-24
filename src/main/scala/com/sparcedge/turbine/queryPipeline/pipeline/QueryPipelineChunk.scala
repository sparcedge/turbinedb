package com.sparcedge.turbine.query.pipeline

// TODO: Update Reducer!
class QueryPipelineChunk(elements: Array[QueryPipelineElement], reducer: Reducer) {
	
	def apply(key: String) {
		var cnt = 0
		while(cnt < elements.length) {
			elements(cnt)(key)
			cnt += 1
		}
		// TODO: Update Reducer Here
	}

	def apply(key: String, value: Double) {
		var cnt = 0
		while(cnt < elements.length) {
			elements(cnt)(key, value)
			cnt += 1
		}
		// TODO: Update Reducer Here
	}

	def apply(key: String, value: String) {
		var cnt = 0
		while(cnt < elements.length) {
			elements(cnt)(key, value)
			cnt += 1
		}
		// TODO: Update Reducer Here
	}

	def apply(ts: Long) {
		var cnt = 0
		while(cnt < elements.length) {
			elements(cnt)(ts)
			cnt += 1
		}
		// TODO: Update Reducer Here
	}

	def processEvent() {
		var cnt = 0
		var continue = true
		while(cnt < elements.length && continue) {
			val elem = elements(cnt)
			if(!elem.shouldContinue) {
				continue = false
			} else if(elem.shouldExtend) {
				extendSubsequentElements(elem.extendKey, elem.extendValue, cnt)
			}
		}
		// TODO: Process Reducer Here
	}

	def extendSubsequentElements(key: String, value: Double, idx: Int) {
		var cnt = idx + 1
		while(cnt < elements.count) {
			elements(cnt)(key, value)
			cnt += 1
		}
		// TODO: Update Reducer Here
	}

}