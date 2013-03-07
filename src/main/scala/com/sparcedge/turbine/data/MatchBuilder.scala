package com.sparcedge.turbine.data

import com.sparcedge.turbine.query.Match
import com.sparcedge.turbine.behaviors.IncrementalBuildBehavior

class MatchBuilder(matches: Iterable[Match]) extends IncrementalBuildBehavior[Match,Boolean] {
	val defaultValue: Boolean = false
	init(matches map { mtch => (mtch.segment -> mtch) })

	def applyNone(idx: Int, mtch: Match): Boolean = false
	def applyNumeric(idx: Int, mtch: Match, num: Double): Boolean = mtch(num)
	def applyString(idx: Int, mtch: Match, str: String): Boolean = mtch(str)
	def applyLong(idx: Int, mtch: Match, lng: Long): Boolean = mtch(lng)

	def satisfiesAllMatches(): Boolean = getValues().forall { v => v }
}