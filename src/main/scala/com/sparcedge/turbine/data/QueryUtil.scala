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
	val GROUPING_LENGTH = 7 // 100000✈

	def eventMatchesAllCriteria(event: Event, matches: Iterable[Match]): Boolean = {
		matches foreach { matcher =>
			if(!matcher.evaluate(event)) {
				return false
			}
		}
		true
	}

	def createGroupString(event: Event, groupings: Iterable[Grouping]): String = {
		if(groupings.size > 0) {
			val builder = new StringBuilder
			builder.append(groupings.head.evaluate(event))
			groupings.tail foreach { grouping =>
				builder.append(GROUP_SEPARATOR).append(grouping.evaluate(event))
			}
			builder.toString
		} else {
			""
		}
	}

	def createGroupString(grpValues: Array[String]): String = {
		val builder = new StringBuilder
		if(grpValues.length > 0) {
			builder.append(grpValues(0))
			var cnt = 1
			while(cnt < grpValues.length) {
				builder.append(GROUP_SEPARATOR).append(grpValues(cnt))
				cnt += 1
			}
		}
		builder.toString
	}

	def extendEvent(event: Event, extenders: Iterable[Extend]): Event = {
		val extendResults = extenders.filter(_.satisfied(event)).map(e => (e.out -> e.evaluate(event)))

		if(extendResults.size > 0) {
			var newDblValues = event.dblValues
			extendResults foreach { res =>
				newDblValues += res
			}
			event.copy(dblValues = newDblValues)
		} else {
			event
		}
	}
}