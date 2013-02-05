package com.sparcedge.turbine.data

import scala.collection.mutable

class IndexUpdateBuilder(indexes: Iterable[Index]) {

	val idxArr = indexes.toArray
	val idxValues = new Array[Double](idxArr.length)
	val idxUpdateInds = new Array[Boolean](idxArr.length)
	val idxIndexMap = toMutableMap(indexes.map(_.indexKey.reducer.segment).zipWithIndex)

	def applySegment(segment: String, value: String) {
		val idxIndex = idxIndexMap.getOrElseUpdate(segment, -1)
		if(idxIndex >= 0) {
			idxUpdateInds(idxIndex) = true
			idxValues(idxIndex) = 1.0
		}
	}

	def applySegment(segment: String, value: Double) {
		val idxIndex = idxIndexMap.getOrElseUpdate(segment, -1)
		if(idxIndex >= 0) {
			idxUpdateInds(idxIndex) = true
			idxValues(idxIndex) = value
		}
	}

	def reset() {
		var cnt = 0
		while(cnt < idxUpdateInds.length) {
			idxUpdateInds(cnt) = false
			cnt += 1
		}
	}

	def executeUpdates(grpStr: String) {
		var cnt = 0
		while(cnt < idxValues.length) {
			if(idxUpdateInds(cnt)) {
				idxArr(cnt).updateUnchecked(idxValues(cnt), grpStr)
			}
			cnt += 1
		}
	}

	def toMutableMap(segIndexes: Iterable[(String,Int)]): mutable.Map[String,Int] = {
		val dest = collection.mutable.Map[String,Int]()
		segIndexes foreach { case (seg, idx) =>
			dest(seg) = idx
		}
		dest
	}
}