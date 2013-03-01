package com.sparcedge.turbine.data

import scala.collection.mutable
import com.sparcedge.turbine.query.{Grouping,Blade}
import QueryUtil._

class GroupStringBuilder(dataGrouping: Grouping, groupings: Iterable[Grouping], blade: Blade) {

	val grpArr = groupings.toArray
	val grpValues = new Array[String](grpArr.length)
	val grpIndexMap = toMutableMap(groupings.map(_.segment).zipWithIndex)
	var dataGrpValue = ""

	def applyTimestamp(ts: Long) {
		val grpIndex = grpIndexMap.getOrElseUpdate("ts", -1)
		dataGrpValue = dataGrouping(ts, blade.periodStartMS)

		if(grpIndex >= 0) {
			val grp = grpArr(grpIndex)
			grpValues(grpIndex) = grp(ts)
		}
	}

	def applySegment(segment: String, value: String) {
		val grpIndex = grpIndexMap.getOrElseUpdate(segment, -1)
		if(grpIndex >= 0) {
			grpValues(grpIndex) = value
		}
	}

	def applySegment(segment: String, value: Double) {
		val grpIndex = grpIndexMap.getOrElseUpdate(segment, -1)
		if(grpIndex >= 0) {
			grpValues(grpIndex) = value.toString
		}
	}

	def applySegment(segment: String) {
		val grpIndex = grpIndexMap.getOrElseUpdate(segment, -1)
		if(grpIndex >= 0) {
			grpValues(grpIndex) = "nil"
		}
	}

	def buildGroupString(): String = {
		createGroupString(dataGrpValue, grpValues)
	}

	def toMutableMap(segIndexes: Iterable[(String,Int)]): mutable.Map[String,Int] = {
		val dest = collection.mutable.Map[String,Int]()
		segIndexes foreach { case (seg, idx) =>
			dest(seg) = idx
		}
		dest
	}
}