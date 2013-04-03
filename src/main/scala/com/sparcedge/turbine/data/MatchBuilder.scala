package com.sparcedge.turbine.data

import scala.collection.mutable.ArrayBuffer

import com.sparcedge.turbine.query.Match
import com.sparcedge.turbine.behaviors.IncrementalBuildBehavior

object MatchBuilder {
	def apply(matches: Iterable[Match]): MatchBuilder = {
		val matchWrappers = matches.map(new MatchWrapper(_))
		new MatchBuilder(matchWrappers)
	}
}

class MatchBuilder(matchWrappers: Iterable[MatchWrapper]) 
		extends IncrementalBuildBehavior[MatchWrapper](matchWrappers.map(mw => (mw -> List(mw.mtch.segment)))) {

	val mtchArr = matchWrappers.toArray
	def createElementArray(): Array[MatchWrapper] = Array[MatchWrapper]()
	def appendElementArray(arr: Array[MatchWrapper], elem: MatchWrapper): Array[MatchWrapper] = arr :+ elem

	def applyNone(key: String, wrapper: MatchWrapper) { wrapper() }
	def applyNumeric(key: String, wrapper: MatchWrapper, num: Double) { wrapper(num) }
	def applyString(key: String, wrapper: MatchWrapper, str: String) { wrapper(str) }
	def applyLong(key: String, wrapper: MatchWrapper, lng: Long) = { wrapper(lng) }

	def satisfiesAllMatches(): Boolean = {
		var cnt = 0
		var satisfiesAll = true
		while(satisfiesAll && cnt < mtchArr.length) {
			satisfiesAll = mtchArr(cnt).value
			cnt += 1
		}
		satisfiesAll
	}
}

class MatchWrapper(val mtch: Match) {
	var value = false
	def apply() { value = false }
	def apply(num: Double) { value = mtch(num) }
	def apply(str: String) { value = mtch(str) }
	def apply(lng: Long) { value = mtch(lng) }
}