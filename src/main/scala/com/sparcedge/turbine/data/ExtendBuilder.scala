package com.sparcedge.turbine.data

import collection.JavaConverters._
import java.util.HashMap

import com.sparcedge.turbine.behaviors.IncrementalBuildBehavior
import com.sparcedge.turbine.query.Extend

object ExtendBuilder {
	def apply(extenders: Iterable[Extend]): ExtendBuilder = {
		val extendWrappers = extenders.map(new ExtendWrapper(_))
		new ExtendBuilder(extendWrappers)
	}
}

class ExtendBuilder(extendWrappers: Iterable[ExtendWrapper]) 
		extends IncrementalBuildBehavior[ExtendWrapper](extendWrappers.map(ew => (ew -> ew.extend.segmentIndexMap.keys))) {

	val extArr = extendWrappers.toArray
	val valArr = new Array[(String,Double)](extArr.length)
	def createElementArray(): Array[ExtendWrapper] = Array[ExtendWrapper]()
	def appendElementArray(arr: Array[ExtendWrapper], elem: ExtendWrapper): Array[ExtendWrapper] = arr :+ elem

	def applyNone(key: String, wrapper: ExtendWrapper) = { /* Nothing */ }
	def applyNumeric(key: String, wrapper: ExtendWrapper, num: Double) { wrapper(key, num) }
	def applyString(key: String, wrapper: ExtendWrapper, str: String) { /* Nothing */ }
	def applyLong(key: String, wrapper: ExtendWrapper, lng: Long) { wrapper(key, lng.toDouble) }

	def extensionValues(): Array[(String,Double)] = {
		var cnt = 0
		while(cnt < extArr.length) {
			val ext = extArr(cnt)
			if(ext.hasValue()) {
				valArr(cnt) = (ext.extend.out -> ext.getValue())
			} else {
				valArr(cnt) = null
			}
			cnt += 1
		}
		valArr
	}

	def reset() {
		var cnt = 0
		while(cnt < extArr.length) {
			extArr(cnt).reset()
			cnt += 1
		}
	}
}

class ExtendWrapper(val extend: Extend) {
	val segmentIndexMap: HashMap[String,Int] = convertToHashMap(extend.segmentIndexMap)
	val valArr = new Array[Double](segmentIndexMap.size)
	var appliedSegments = 0

	def apply(segment: String, value: Double) {
		if(segmentIndexMap.containsKey(segment)) {
			val idx = segmentIndexMap.get(segment)
			valArr(idx) = value
			appliedSegments += 1
		}
	}

	def reset() {
		appliedSegments = 0
	}

	def hasValue(): Boolean = {
		appliedSegments == valArr.length
	}

	def getValue(): Double = {
		extend.evaluate(valArr)
	}

	def convertToHashMap(map: Map[String,Int]): HashMap[String,Int] = {
		val hmap = new HashMap[String,Int]()
		map foreach { case (key, value) =>
			hmap.put(key, value)
		}
		hmap
	}
}