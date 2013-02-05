package com.sparcedge.turbine.query

import com.sparcedge.turbine.data.IndexKey
import scala.collection.immutable.TreeMap
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.json4s._
import org.json4s.jackson.JsonMethods._

object TurbineQuery {

	implicit val formats = org.json4s.DefaultFormats

	def apply(queryStr: String): TurbineQuery = {
		val json = parse(queryStr)
		json.extract[TurbineQuery]
	}
}

case class TurbineQuery(blade: Blade, query: Query)

case class Query (
	category: String,
	range: TimeRange,
	`match`: Option[Map[String,JValue]],
	group: Option[List[Grouping]],
	reduce: Option[Reduce]
) {
	implicit val formats = org.json4s.DefaultFormats
	val minuteFormatter = DateTimeFormat.forPattern("yyyy-MM-dd-HH-mm")
	val startMinute = minuteFormatter.print(new DateTime(range.start))
	val startPlusMinute = minuteFormatter.print(new DateTime(range.start).plusMinutes(1))
	val startPlusMinuteDate = minuteFormatter.parseDateTime(startPlusMinute)
	val endMinute = range.end.map { end: Long => minuteFormatter.print(new DateTime(end)) }
	val endMinuteDate = endMinute.map(minuteFormatter.parseDateTime(_))
	val orderedMatches = `match`.map(unorderedMatches => TreeMap(unorderedMatches.toArray:_*))

	def createAggregateCacheString(reducer: Reducer): String = {
		orderedMatches.mkString + "-" + group.mkString + "-" + reducer.segment + "-" + reducer.reducer
	}

	def createAggregateIndexKey(reducer: Reducer): IndexKey = {
		IndexKey(reducer, matches, groupings)
	}

	def retrieveRequiredFields(): Set[String] = {
		var reqFields = Set[String]()
		reqFields = reqFields ++ matches.map(_.segment)
		reqFields = reqFields ++ groupings.filter(_.`type` == "segment").flatMap(_.value)
		reqFields = reqFields ++ reduce.map(_.reducerList map (_.segment)).getOrElse(List[String]())
		reqFields
	}

	val matches = `match`.getOrElse(Map[String,JValue]()) map { case (segment, value) => 
		new Match(segment, value.extract[Map[String,JValue]])
	}
	lazy val requiredFields = retrieveRequiredFields()
	val groupings = group.getOrElse(List[Grouping]())
}

case class TimeRange (start: Long, end: Option[Long])