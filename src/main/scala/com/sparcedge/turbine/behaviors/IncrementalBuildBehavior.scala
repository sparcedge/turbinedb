package com.sparcedge.turbine.behaviors

import java.util.HashMap
import scala.collection.mutable

// TODO: Only pass indexes to implementers that want it
trait IncrementalBuildBehavior[T,O] {
	def defaultValue: O
	val idxMap = new HashMap[String,List[Int]]()
	var elements = mutable.ArrayBuffer[T]()
	var values = mutable.ArrayBuffer[O]()

	def init(pairs: Iterable[(String,T)]) {
		var cnt = 0
		pairs foreach { case (key, elem) =>
			var idxs = idxMap.get(key)
			if(idxs == null) {
				idxs = List[Int]()	
			}
			idxMap.put(key, cnt :: idxs)
			elements += elem
			values += defaultValue
			cnt += 1
		}
	}

	def apply(key: String) {
		val idxs = idxMap.get(key)
		if(idxs != null) {
			idxs.foreach { idx =>
				values(idx) = applyNone(idx, elements(idx))
			}
		}
	}

	def apply(key: String, lng: Long) {
		val idxs = idxMap.get(key)
		if(idxs != null) {
			idxs.foreach { idx =>
				values(idx) = applyLong(idx, elements(idx), lng)
			}
		}
	}

	def apply(key: String, num: Double) {
		val idxs = idxMap.get(key)
		if(idxs != null) {
			idxs.foreach { idx =>
				values(idx) = applyNumeric(idx, elements(idx), num)
			}
		}
	} 
	
	def apply(key: String, str: String) {
		val idxs = idxMap.get(key)
		if(idxs != null) {
			idxs.foreach { idx =>
				values(idx) = applyString(idx, elements(idx), str)
			}
		}
	}

	def getValues(): mutable.ArrayBuffer[O] = values

	def applyNone(idx: Int, elem: T): O
	def applyLong(idx: Int, elem: T, lng: Long): O
	def applyNumeric(idx: Int, elem: T, num: Double): O
	def applyString(idx: Int, elem: T, str: String): O
}