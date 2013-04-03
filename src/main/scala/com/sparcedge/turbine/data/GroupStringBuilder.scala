package com.sparcedge.turbine.data

import scala.collection.mutable.ArrayBuffer

import com.sparcedge.turbine.query.Grouping
import com.sparcedge.turbine.Blade
import com.sparcedge.turbine.behaviors.IncrementalBuildBehavior
import QueryUtil._

object GroupStringBuilder {
	def apply(groupings: Iterable[Grouping], blade: Blade): GroupStringBuilder = {
		val groupWrappers = groupings.zipWithIndex.map { case (grouping,idx) => new GroupingWrapper(grouping, idx)}
		new GroupStringBuilder(groupWrappers, blade)
	}
}

class GroupStringBuilder(groupWrappers: Iterable[GroupingWrapper], blade: Blade)
		extends IncrementalBuildBehavior[GroupingWrapper](groupWrappers.map(gw => (gw -> List(gw.grouping.segment)))) {

	var dataGrpValue = ""
	val values = new Array[String](groupWrappers.size)
	def createElementArray(): Array[GroupingWrapper] = Array[GroupingWrapper]()
	def appendElementArray(arr: Array[GroupingWrapper], elem: GroupingWrapper): Array[GroupingWrapper] = arr :+ elem

	def applyNone(key: String, wrapper: GroupingWrapper) { values(wrapper.idx) = "nil" }
	def applyNumeric(key: String, wrapper: GroupingWrapper, num: Double) { values(wrapper.idx) = wrapper.grouping(num) }
	def applyString(key: String, wrapper: GroupingWrapper, str: String) { values(wrapper.idx) = wrapper.grouping(str) }
	def applyLong(key: String, wrapper: GroupingWrapper, lng: Long) { values(wrapper.idx) = wrapper.grouping(lng) }

	override def apply(key: String, lng: Long) {
		dataGrpValue = DATA_GROUPING(lng, blade.periodStartMS)
		super.apply(key,lng)
	}

	def buildGroupString(): String = {
		createGroupString(dataGrpValue, values)
	}
}

class GroupingWrapper(val grouping: Grouping, val idx: Int)