package com.sparcedge.turbine.behaviors

import java.util.HashMap
import scala.collection.mutable.ArrayBuffer

// Warning Performance Critical Path (Here be dargons)
trait IncrementalBuildBehavior[T, /*@specialized(Double)*/ O] {
	def defaultValue: O
	val idxMap = new HashMap[String,Array[Int]]()
	var elements: Array[T] = null
	var values: Array[O] = null

	def init(pairs: Iterable[(String,T)]) {
		var cnt = 0
		val elms = ArrayBuffer[T]()
		val vals = ArrayBuffer[O]()
		pairs foreach { case (key, elem) =>
			var idxs = idxMap.get(key)
			if(idxs == null) {
				idxs = Array[Int]()
			}
			idxs = idxs :+ cnt
			idxMap.put(key, idxs)
			elms += elem
			vals += defaultValue
			cnt += 1
		}
		elements = makeElementArray(elms)
		values = makeValArray(vals)
	}

	def apply(key: String) {
		val idxs = idxMap.get(key)
		if(idxs != null) {
			var cnt = 0
			while (cnt < idxs.length) {
				val idx = idxs(cnt)
				values(idx) = applyNone(idx, elements(idx))
				cnt += 1
			}
		}
	}

	def apply(key: String, lng: Long) {
		val idxs = idxMap.get(key)
		if(idxs != null) {
			var cnt = 0
			while (cnt < idxs.length) {
				val idx = idxs(cnt)
				values(idx) = applyLong(idx, elements(idx), lng)
				cnt += 1
			}
		}
	}

	def apply(key: String, num: Double) {
		val idxs = idxMap.get(key)
		if(idxs != null) {
			var cnt = 0
			while (cnt < idxs.length) {
				val idx = idxs(cnt)
				values(idx) = applyNumeric(idx, elements(idx), num)
				cnt += 1
			}
		}
	} 
	
	def apply(key: String, str: String) {
		val idxs = idxMap.get(key)
		if(idxs != null) {
			var cnt = 0
			while (cnt < idxs.length) {
				val idx = idxs(cnt)
				values(idx) = applyString(idx, elements(idx), str)
				cnt += 1
			}
		}
	}

	def makeValArray(values: ArrayBuffer[O]): Array[O]
	def makeElementArray(elements: ArrayBuffer[T]): Array[T]
	def getValues(): Array[O] = values

	def applyNone(idx: Int, elem: T): O
	def applyLong(idx: Int, elem: T, lng: Long): O
	def applyNumeric(idx: Int, elem: T, num: Double): O
	def applyString(idx: Int, elem: T, str: String): O
}