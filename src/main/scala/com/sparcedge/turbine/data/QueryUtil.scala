package com.sparcedge.turbine.data

import scala.collection.mutable
import scala.collection.GenMap
import com.sparcedge.turbine.query._
import com.sparcedge.turbine.event.{Event,ConcreteEvent}
import com.sparcedge.turbine.util.{Timer,WrappedTreeMap}

object QueryUtil {
	val GROUP_SEPARATOR = "✈"
	val GROUP_SEPARATOR_CHAR = '✈'
	val GROUPING_LENGTH = 5 // Mod 100000
	val aggregateGrouping = Grouping("duration", Some("ihour"))

	def eventMatchesAllCriteria(event: Event, matches: Iterable[Match]): Boolean = {
		matches foreach { matcher =>
			if(!matcher(event)) {
				return false
			}
		}
		true
	}

	def createGroupStringForEvent(event: Event, groupings: Iterable[Grouping]): String = {
		if(groupings.size > 0) {
			val builder = new StringBuilder
			builder.append(groupings.head.createGroup(event))
			groupings.tail foreach { grouping =>
				builder.append(GROUP_SEPARATOR).append(grouping.createGroup(event))
			}
			builder.toString
		} else {
			""
		}
	}

	def createGroupString(dataGrpValue: String, grpValues: Array[String]): String = {
		val builder = new StringBuilder
		builder.append(dataGrpValue)
		var cnt = 0
		while(cnt < grpValues.length) {
			builder.append(GROUP_SEPARATOR).append(grpValues(cnt))
			cnt += 1
		}
		builder.toString
	}
}