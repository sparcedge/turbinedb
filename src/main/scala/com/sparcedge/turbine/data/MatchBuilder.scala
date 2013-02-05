package com.sparcedge.turbine.data

import scala.collection.mutable
import com.sparcedge.turbine.query.Match

class MatchBuilder(matches: Iterable[Match]) {

	val matchArr = matches.toArray
	val matchResults = new Array[Boolean](matchArr.length)
	val matchIndexMap = toMutableMap(matches.map(_.segment).zipWithIndex)
	
	def applyTimestamp(ts: Long) {
		val matchIndex = matchIndexMap.getOrElseUpdate("ts", -1)

		if(matchIndex >= 0) {
			val mtch = matchArr(matchIndex)
			matchResults(matchIndex) = mtch(ts)
		}
	}

	def applySegment(segment: String, value: String) {
		val matchIndex = matchIndexMap.getOrElseUpdate(segment, -1)

		if(matchIndex >= 0) {
			val mtch = matchArr(matchIndex)
			matchResults(matchIndex) = mtch(value)
		}
	}

	def applySegment(segment: String, value: Double) {
		val matchIndex = matchIndexMap.getOrElseUpdate(segment, -1)

		if(matchIndex >= 0) {
			val mtch = matchArr(matchIndex)
			matchResults(matchIndex) = mtch(value)
		}
	}

	def applySegment(segment: String) {
		val matchIndex = matchIndexMap.getOrElseUpdate(segment, -1)

		if(matchIndex >= 0) {
			matchResults(matchIndex) = false
		}
	}

	def satisfiesAllMatches(): Boolean = {
		var cnt = 0
		while(cnt < matchResults.length) {
			if(!matchResults(cnt)) {
				return false
			}
			cnt += 1
		}
		true
	}

	def toMutableMap(segIndexes: Iterable[(String,Int)]): mutable.Map[String,Int] = {
		val dest = collection.mutable.Map[String,Int]()
		segIndexes foreach { case (seg, idx) =>
			dest(seg) = idx
		}
		dest
	}
}