package com.sparcedge.turbine.blade

import org.scala_tools.time.Imports._
import org.joda.time.DateTime
import net.liftweb.json
import json._

object TurbineAnalyticsQuery {

	implicit val formats = Serialization.formats(NoTypeHints)

	def parse(queryStr: String): TurbineAnalyticsQuery = {
		val jsonObj = json.parse(queryStr)
		jsonObj.extract[TurbineAnalyticsQuery]
	}

}

case class TurbineAnalyticsQuery (
	blade: Blade,
	query: Query
) {
	def createCacheSegmentString(): String = {
		blade.domain + "." + blade.tenant + "." + blade.category + "." + blade.period
	}
}

case class Blade (
	domain: String,
	tenant: String,
	category: String,
	period: String
)

case class Query (
	category: String,
	range: TimeRange,
	`match`: Option[Map[String,JObject]],
	group: Option[List[Grouping]],
	reduce: Option[Reduce]
)

case class TimeRange (
	start: Long,
	end: Option[Long]
)

case class Grouping (
	duration: Option[String],
	resource: Option[Boolean],
	segment: Option[String]
)

case class Reduce (
	reducers: Option[List[Reducer]],
	filter: Option[Map[String,JObject]]
)

case class Reducer (
	propertyName: String,
	reducer: String,
	segment: String
)