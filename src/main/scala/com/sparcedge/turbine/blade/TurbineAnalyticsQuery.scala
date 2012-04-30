package com.sparcedge.turbine.blade

import org.scala_tools.time.Imports._
import org.joda.time.DateTime
import net.liftweb.json._

object TurbineAnalyticsQuery {

	implicit val formats = Serialization.formats(NoTypeHints)

	def parse(queryStr: String): TurbineAnalyticsQuery = {
		val json = net.liftweb.json.parse(queryStr)
		json.extract[TurbineAnalyticsQuery]
	}

}

class TurbineAnalyticsQuery (
	val domain: String,
	val tenant: String,
	val category: String,
	val monthSegment: String,
	val start: Long,
	val end: Long
) {
	def createCacheSegmentString(): String = {
		domain + "." + tenant + "." + category + "." + monthSegment
	}
}