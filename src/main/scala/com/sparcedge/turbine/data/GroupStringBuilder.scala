package com.sparcedge.turbine.data

import scala.collection.mutable.ArrayBuffer

import com.sparcedge.turbine.query.Grouping
import com.sparcedge.turbine.Blade
import com.sparcedge.turbine.behaviors.IncrementalBuildBehavior
import QueryUtil._

class GroupStringBuilder(groupings: Iterable[Grouping], blade: Blade) extends IncrementalBuildBehavior[Grouping,String] {
	var dataGrpValue = ""
	val defaultValue: String = "nil"
	var values: Array[String] = new Array[String](0)
	init(groupings map { grouping => (grouping.segment -> grouping) })

	def makeValArray(values: ArrayBuffer[String]): Array[String] = values.toArray
	def makeElementArray(elements: ArrayBuffer[Grouping]): Array[Grouping] = elements.toArray

	def applyNone(idx: Int, grouping: Grouping): String = "nil"
	def applyNumeric(idx: Int, grouping: Grouping, num: Double): String = grouping(num)
	def applyString(idx: Int, grouping: Grouping, str: String): String = grouping(str)
	def applyLong(idx: Int, grouping: Grouping, lng: Long): String = grouping(lng)

	override def apply(key: String, lng: Long) {
		dataGrpValue = DATA_GROUPING(lng, blade.periodStartMS)
		super.apply(key,lng)
	}

	def buildGroupString(): String = {
		createGroupString(dataGrpValue, getValues)
	}
}