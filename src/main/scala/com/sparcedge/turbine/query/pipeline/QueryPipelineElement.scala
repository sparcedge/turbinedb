package com.sparcedge.turbine.query.pipeline

import collection.JavaConverters._
import java.util.HashMap
import com.sparcedge.turbine.query.{Extend,Match}

abstract class QueryPipelineElement {
	def shouldContinue: Boolean = true
	def shouldExtend: Boolean = false
	def extendKey: String = "UNDEFINED"
	def extendValue: Double = 0.0

	def reset()
	def apply(key: String)
	def apply(key: String, value: Double)
	def apply(key: String, value: String)
	def apply(ts: Long)
}

class MatchPipelineElement(mtch: Match) extends QueryPipelineElement {
	var satisfied = false
	val segment = mtch.segment

	override def shouldContinue(): Boolean = {
		satisfied
	}

	def apply(key: String) { 
		satisfied = key != segment 
	}

	def apply(key: String, value: Double) {
		if(key == segment) {
			satisfied = mtch(value)
		}
	}

	def apply(key: String, value: String) {
		if(key == segment) {
			satisfied = mtch(value)
		}
	}

	def applyTimestamp(ts: Long) { 
		/* Matches Don't Act on Timestamps */ 
	}

	def reset() { 
		satisfied = false 
	}
}

class ExtendPipelineElement(extend: Extend) extends QueryPipelineElement {
	val output = extend.out
	val valArr = new Array[Double](segmentIndexMap.size)
	val segmentIndexMap = convertToHashMap(extend.segmentIndexMap)
	var appliedSegments = 0

	override def shouldExtend(): Boolean = {
		appliedSegments == valArr.length
	}

	override def extendKey(): Boolean = {
		output
	}

	override def extendValue(): Double = {
		extend.extendValue(valArr)
	}

	def apply(key: String) { 
		/* Only Updates With Double Values */ 
	}

	def apply(key: String, value: String) { 
		/* Only Updates With Double Values */ 
	}

	def apply(key: String, value: Double) {
		if(segmentIndexMap.containsKey(key)) {
			val idx = segmentIndexMap.get(key)
			valArr(idx) = value
			appliedSegments += 1
		}
	}

	def reset() { 
		appliedSegments = 0 
	}

	def convertToHashMap(map: Map[String,Int]): HashMap[String,Int] = {
		val hmap = new HashMap[String,Int]()
		map foreach { case (key, value) =>
			hmap.put(key, value)
		}
		hmap
	}
}