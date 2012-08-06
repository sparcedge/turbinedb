package com.sparcedge.turbine.blade.query

import scala.collection.immutable.TreeMap
import net.liftweb.json._
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import com.mongodb.casbah.query.Imports._

object TurbineAnalyticsQuery {

	implicit val formats = Serialization.formats(NoTypeHints)

	def apply(queryStr: String): TurbineAnalyticsQuery = {
		val jsonObj = parse(queryStr)
		jsonObj.extract[TurbineAnalyticsQuery]
	}
}

case class TurbineAnalyticsQuery(blade: Blade, query: Query, qid: String) {
	def createCacheSegmentString(): String = {
		blade.domain + "." + blade.tenant + "." + blade.category + "." + blade.period
	}
}

case class Blade(domain: String, tenant: String, category: String, period: String) {
	val formatter = DateTimeFormat.forPattern("yyyy-MM")
	val periodStart = formatter.parseDateTime(period)
	val periodEnd = periodStart.plusMonths(1)
}

case class Query (
	category: String,
	range: TimeRange,
	`match`: Option[Map[String,JValue]],
	group: Option[List[Grouping]],
	reduce: Option[Reduce]
) {
	val hourFormatter = DateTimeFormat.forPattern("yyyy-MM-hh")
	val startHour = hourFormatter.print(new DateTime(range.start))
	val startPlusHour = hourFormatter.print(new DateTime(range.start).plusHours(1))
	val startPlusHourDate = hourFormatter.parseDateTime(startPlusHour)
	val endHour = range.end.map { end: Long => hourFormatter.print(new DateTime(end)) }
	val endHourDate = endHour.map(hourFormatter.parseDateTime(_))
	val orderedMatches = `match`.map(unorderedMatches => TreeMap(unorderedMatches.toArray:_*))

	def createAggregateCacheString(reducer: Reducer): String = {
		orderedMatches.mkString + "-" + group.mkString + "-" + reducer.segment + "-" + reducer.reducer
	}

	def retrieveRequiredFields(): Set[String] = {
		var reqFields = Set[String]()
		reqFields = reqFields ++ matches.map(_.segment)
		reqFields = reqFields ++ groupings.filter(_.`type` == "segment").flatMap(_.value)
		reqFields = reqFields ++ reduce.map(_.reducerList map (_.segment)).getOrElse(List[String]())
		reqFields
	}

	implicit val formats = Serialization.formats(NoTypeHints)
	val matches = `match`.getOrElse(Map[String,JValue]()) map { case (segment, value) => 
		new Match(segment, value.extract[Map[String,JValue]])
	}
	lazy val requiredFields = retrieveRequiredFields()
	val groupings = group.getOrElse(List[Grouping]())
}

case class TimeRange (start: Long, end: Option[Long])