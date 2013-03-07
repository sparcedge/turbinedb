package com.sparcedge.turbine.data

import com.sparcedge.turbine.behaviors.IncrementalBuildBehavior

// TODO: Be able to handle non numeric reduce cases
class IndexUpdateBuilder(indexes: Iterable[Index]) extends IncrementalBuildBehavior[Index,Double] {
	init(indexes map { index => (index.indexKey.reducer.segment -> index) })
	val defaultValue: Double = 0.0
	val updateInds = new Array[Boolean](getValues().size)

	def applyNone(idx: Int, index: Index): Double = 0.0
	def applyNumeric(idx: Int, index: Index, num: Double): Double = {
		updateInds(idx) = true
		num
	}
	def applyString(idx: Int, index: Index, str: String): Double = {
		updateInds(idx) = index.indexKey.reducer.reduceType == "count"
		0.0
	}
	def applyLong(idx: Int, index: Index, lng: Long): Double = {
		updateInds(idx) = true
		lng.toDouble
	}

	def reset() {
		var cnt = 0
		while(cnt < updateInds.length) {
			updateInds(cnt) = false
			cnt += 1
		}
	}

	def executeUpdates(grpStr: String) {
		var cnt = 0
		getValues() foreach { value =>
			if(updateInds(cnt)) {
				elements(cnt).updateUnchecked(value, grpStr)
			}
			cnt += 1
		}
	}
}