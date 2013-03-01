package com.sparcedge.turbine.data

import scala.collection.mutable
import scala.collection.GenMap

import com.sparcedge.turbine.query._
import com.sparcedge.turbine.event.Event
import com.sparcedge.turbine.util.WrappedTreeMap
import com.sparcedge.turbine.Blade

object QueryUtil {
	val GROUP_SEPARATOR = "✈"
	val GROUP_SEPARATOR_CHAR = '✈'
	val GROUPING_LENGTH = 6 // 100000
	val DATA_GROUPING = new IndexGrouping("hour")

	def eventMatchesAllCriteria(event: Event, matches: Iterable[Match]): Boolean = {
		matches foreach { matcher =>
			if(!matcher(event)) {
				return false
			}
		}
		true
	}

	def createDataGroupString(event: Event, blade: Blade, groupings: Iterable[Grouping]): String = {
		new StringBuilder(DATA_GROUPING(event.ts, blade.periodStartMS)).append(createGroupString(event,groupings)).toString
	}

	def createGroupString(event: Event, groupings: Iterable[Grouping]): String = {
		if(groupings.size > 0) {
			val builder = new StringBuilder
			builder.append(groupings.head(event))
			groupings.tail foreach { grouping =>
				builder.append(GROUP_SEPARATOR).append(grouping(event))
			}
			builder.toString
		} else {
			""
		}
	}

	def createGroupString(dataGrpValue: String, grpValues: Array[String]): String = {
		val builder = new StringBuilder
		builder.append(dataGrpValue)
		if(grpValues.length > 0) {
			builder.append(grpValues(0))
		}
		var cnt = 1
		while(cnt < grpValues.length) {
			builder.append(GROUP_SEPARATOR).append(grpValues(cnt))
			cnt += 1
		}
		builder.toString
	}
}