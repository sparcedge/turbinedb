package com.sparcedge.turbine.data

import scala.collection.mutable.ArrayBuffer
import com.sparcedge.turbine.behaviors.IncrementalBuildBehavior

object IndexUpdateBuilder {
	def apply(indexes: Iterable[Index]): IndexUpdateBuilder = {
		val indexWrappers = indexes.map(new IndexWrapper(_))
		new IndexUpdateBuilder(indexWrappers)
	}
}

class IndexUpdateBuilder(indexWrappers: Iterable[IndexWrapper]) 
		extends IncrementalBuildBehavior[IndexWrapper](indexWrappers.map(iw => (iw -> List(iw.index.indexKey.reducer.segment)))) {

	var indexesArr = indexWrappers.toArray
	def createElementArray(): Array[IndexWrapper] = Array[IndexWrapper]()
	def appendElementArray(arr: Array[IndexWrapper], elem: IndexWrapper): Array[IndexWrapper] = arr :+ elem

	def applyNone(key: String, wrapper: IndexWrapper) { /* No Update */ }
	def applyNumeric(key: String, wrapper: IndexWrapper, num: Double) { wrapper(num) }
	def applyLong(key: String, wrapper: IndexWrapper, lng: Long) { wrapper(lng.toDouble) }

	def applyString(key: String, wrapper: IndexWrapper, str: String) {
		if(wrapper.index.indexKey.reducer.reduceType == "count") { wrapper(1.0) }
	}

	def reset() {
		var cnt = 0
		while(cnt < indexesArr.length) {
			indexesArr(cnt).reset()
			cnt += 1
		}
	}

	def executeUpdates(grpStr: String) {
		var cnt = 0
		while (cnt < indexesArr.length) {
			indexesArr(cnt).update(grpStr)
			cnt += 1
		}
	}
}

class IndexWrapper(val index: Index) {
	private var updated = false
	private var value: Double = 0.0

	def apply(num: Double) {
		value = num
		updated = true
	}

	def update(grpStr: String) {
		if(updated) {
			index.updateUnchecked(value, grpStr)
		}
	}

	def reset() {
		updated = false
	}
}