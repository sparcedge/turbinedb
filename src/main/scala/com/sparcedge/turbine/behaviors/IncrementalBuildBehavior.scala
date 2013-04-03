package com.sparcedge.turbine.behaviors

import java.util.HashMap
import scala.collection.mutable.ArrayBuffer

abstract class IncrementalBuildBehavior[T](elemKeysList: Iterable[(T,Iterable[String])]) {

	val elemMap = createElementMap()
	val hasElements = elemKeysList.size != 0

	private def createElementMap(): HashMap[String,Array[T]] = {
		val eMap = new HashMap[String,Array[T]]()
		elemKeysList foreach { case (elem, keys) =>
			keys foreach { key =>
				var elems = eMap.get(key)
				if(elems == null) {
					elems = createElementArray()
				}
				elems = appendElementArray(elems, elem)
				eMap.put(key, elems)
			}
		}
		eMap
	}

	def apply(key: String) {
		if(hasElements) {
			val elems = elemMap.get(key)
			if(elems != null) {
				var cnt = 0
				while (cnt < elems.length) {
					val elem = elems(cnt)
					applyNone(key, elem)
					cnt += 1
				}
			}
		}
	}

	def apply(key: String, lng: Long) {
		if(hasElements) {
			val elems = elemMap.get(key)
			if(elems != null) {
				var cnt = 0
				while (cnt < elems.length) {
					val elem = elems(cnt)
					applyLong(key, elem, lng)
					cnt += 1
				}
			}
		}
	}

	def apply(key: String, num: Double) {
		if(hasElements) {
			val elems = elemMap.get(key)
			if(elems != null) {
				var cnt = 0
				while (cnt < elems.length) {
					val elem = elems(cnt)
					applyNumeric(key, elem, num)
					cnt += 1
				}
			}
		}
	} 
	
	def apply(key: String, str: String) {
		if(hasElements) {
			val elems = elemMap.get(key)
			if(elems != null) {
				var cnt = 0
				while (cnt < elems.length) {
					val elem = elems(cnt)
					applyString(key, elem, str)
					cnt += 1
				}
			}
		}
	}

	def createElementArray(): Array[T]
	def appendElementArray(arr: Array[T], elem: T): Array[T]

	def applyNone(key: String, elem: T)
	def applyLong(key: String, elem: T, lng: Long)
	def applyNumeric(key: String, elem: T, num: Double)
	def applyString(key: String, elem: T, str: String)
}