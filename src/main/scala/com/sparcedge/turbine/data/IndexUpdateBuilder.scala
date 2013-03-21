package com.sparcedge.turbine.data

import scala.collection.mutable.ArrayBuffer

import com.sparcedge.turbine.behaviors.IncrementalBuildBehavior

// TODO: Be able to handle non numeric reduce cases
class IndexUpdateBuilder(indexes: Iterable[Index]) extends IncrementalBuildBehavior[Index,Double] {
	val defaultValue: Double = 0.0	
	var values: Array[Double] = new Array[Double](0)
	init(indexes map { index => (index.indexKey.reducer.segment -> index) })
	val updateInds = new Array[Boolean](getValues().size)

	def makeValArray(values: ArrayBuffer[Double]): Array[Double] = {
		val arr = new Array[Double](values.length)
		var cnt = 0
		while(cnt < values.length) {
			arr(cnt) = values(cnt)
			cnt += 1
		}
		arr
	}
	def makeElementArray(elements: ArrayBuffer[Index]): Array[Index] = elements.toArray

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
		val values = getValues()
		while (cnt < values.length) {
			if(updateInds(cnt)) {
				elements(cnt).updateUnchecked(values(cnt), grpStr)
			}
			cnt += 1
		}
	}
}