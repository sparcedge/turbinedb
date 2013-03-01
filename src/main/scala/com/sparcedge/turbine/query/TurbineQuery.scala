package com.sparcedge.turbine.query

import scala.util.{Try,Success,Failure}
import scala.collection.immutable.TreeMap
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import play.api.libs.json.Json
import play.api.libs.json.JsObject

import com.sparcedge.turbine.{Blade,Collection}
import com.sparcedge.turbine.data.IndexKey


case class TurbineQueryPackage(collection: Collection, query: TurbineQuery)

object TurbineQuery {
	implicit val turbineQueryFormat = Json.format[TurbineQueryParse]

	def apply(parseQuery: TurbineQueryParse): TurbineQuery = {
		val start = parseQuery.start
		val end = parseQuery.end
		val matches = parseQuery.`match` map { jobjs => jobjs map { jobj => Match(jobj) } } getOrElse (List[Match]())
		val groupings = parseQuery.group map { jobjs => jobjs map { jobj => Grouping(jobj) } } getOrElse (List[Grouping]())
		val reducers = parseQuery.reduce.map(Reducer(_))
		new TurbineQuery(start, end, matches, groupings, reducers)
	}

	def tryParse(queryStr: String): Try[TurbineQuery] = {
		Try {
			val json = Json.parse(queryStr)
			TurbineQuery(json.as[TurbineQueryParse])
		}
	}
}

case class TurbineQueryParse (
	start: Option[Long],
	end: Option[Long],
	`match`: Option[List[JsObject]],
	group: Option[List[JsObject]],
	reduce: List[JsObject]
)

class TurbineQuery (
	val start: Option[Long] = None,
	val end: Option[Long] = None,
	val matches: List[Match] = List[Match](),
	val groupings: List[Grouping] = List[Grouping](),
	val reducers: List[Reducer] = List[Reducer]()
) {
	val minuteFormatter = DateTimeFormat.forPattern("yyyy-MM-dd-HH-mm")
	val startPlusMinute = start map { s => minuteFormatter.print(new DateTime(s).plusMinutes(1)) }
	val endMinute = end map { e => minuteFormatter.print(new DateTime(e)) }

	def createAggregateIndexKey(reducer: Reducer): IndexKey = {
		IndexKey(reducer.getCoreReducer(), matches, groupings)
	}
}