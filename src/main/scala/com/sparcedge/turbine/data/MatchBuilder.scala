package com.sparcedge.turbine.data

import scala.collection.mutable.ArrayBuffer

import com.sparcedge.turbine.query.Match
import com.sparcedge.turbine.behaviors.IncrementalBuildBehavior

class MatchBuilder(matches: Iterable[Match]) extends IncrementalBuildBehavior[Match,Boolean] {
	val defaultValue: Boolean = false
	init(matches map { mtch => (mtch.segment -> mtch) })

	def makeValArray(values: ArrayBuffer[Boolean]): Array[Boolean] = values.toArray
	def makeElementArray(elements: ArrayBuffer[Match]): Array[Match] = elements.toArray

	def applyNone(idx: Int, mtch: Match): Boolean = false
	def applyNumeric(idx: Int, mtch: Match, num: Double): Boolean = mtch(num)
	def applyString(idx: Int, mtch: Match, str: String): Boolean = mtch(str)
	def applyLong(idx: Int, mtch: Match, lng: Long): Boolean = mtch(lng)

	def satisfiesAllMatches(): Boolean = {
		val values = getValues()
		var cnt = 0
		var satisfiesAll = true
		while(satisfiesAll && cnt < values.length) {
			satisfiesAll = values(cnt)
			cnt += 1
		}
		satisfiesAll
	}
}