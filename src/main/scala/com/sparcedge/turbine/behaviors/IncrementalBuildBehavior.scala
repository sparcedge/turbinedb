package com.sparcedge.turbine.behaviors

import scala.collection.mutable

// TODO: Only pass indexes to implementers that want it
trait IncrementalBuildBehavior[T,O] {
	def defaultValue: O
	val idxMap = mutable.Map[String,List[Int]]()
	var elements = mutable.ArrayBuffer[T]()
	var values = mutable.ArrayBuffer[O]()

	def init(pairs: Iterable[(String,T)]) {
		var cnt = 0
		pairs foreach { case (key, elem) =>
			val idxs = idxMap.getOrElseUpdate(key, List[Int]())
			idxMap(key) = cnt :: idxs
			elements += elem
			values += defaultValue
			cnt += 1
		}
	}

	def apply(key: String) {
		idxMap.getOrElseUpdate(key, Nil).foreach { idx =>
			values(idx) = applyNone(idx, elements(idx))
		}
	}

	def apply(key: String, lng: Long) {
		idxMap.getOrElseUpdate(key, Nil).foreach { idx =>
			values(idx) = applyLong(idx, elements(idx), lng)
		}
	}

	def apply(key: String, num: Double) {
		idxMap.getOrElseUpdate(key, Nil).foreach { idx =>
			values(idx) = applyNumeric(idx, elements(idx), num)
		}	
	} 
	
	def apply(key: String, str: String) {
		idxMap.getOrElseUpdate(key, Nil).foreach { idx =>
			values(idx) = applyString(idx, elements(idx), str)
		}
	}

	def getValues(): Iterable[O] = values

	def applyNone(idx: Int, elem: T): O
	def applyLong(idx: Int, elem: T, lng: Long): O
	def applyNumeric(idx: Int, elem: T, num: Double): O
	def applyString(idx: Int, elem: T, str: String): O
}