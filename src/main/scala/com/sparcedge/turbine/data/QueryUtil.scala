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
	// TODO: Not good!
	var DATA_GROUPING = new IndexGrouping("ihour")

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

	def createGroupString(dataGrpValue: String, grpValues: Iterable[String]): String = {
		val builder = new StringBuilder
		builder.append(dataGrpValue)
		if(grpValues.size > 0) {
			builder.append(grpValues.head)
			grpValues.tail foreach { grpVal =>
				builder.append(GROUP_SEPARATOR).append(grpVal)
			}
		}
		builder.toString
	}
}