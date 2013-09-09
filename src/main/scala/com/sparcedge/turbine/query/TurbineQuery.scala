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
		val extenders = parseQuery.extend map { jobjs => jobjs map { jobj => Extend(jobj) } } getOrElse (Vector[Extend]())
		val matches = parseQuery.`match` map { jobjs => jobjs map { jobj => Match(jobj) } } getOrElse (Vector[Match]())
		val groupings = parseQuery.group map { jobjs => jobjs map { jobj => Grouping(jobj) } } getOrElse (Vector[Grouping]())
		val reducers = parseQuery.reduce.map(ReducerPackage(_))
		new TurbineQuery(start, end, extenders, matches, groupings, reducers)
	}

	def tryParse(queryStr: String): Try[TurbineQuery] = {
		Try {
			val json = Json.parse(queryStr)
			TurbineQuery(json.as[TurbineQueryParse])
		}
	}

	def tryParseMatches(matchStr: String): Try[Iterable[Match]] = {
		Try {
			val json = Json.parse(matchStr)
			json.as[Vector[JsObject]].map(Match(_))
		}
	}
}

case class TurbineQueryParse (
	start: Option[Long],
	end: Option[Long],
	extend: Option[Vector[JsObject]],
	`match`: Option[Vector[JsObject]],
	group: Option[Vector[JsObject]],
	reduce: Vector[JsObject]
)

class TurbineQuery (
	val start: Option[Long] = None,
	val end: Option[Long] = None,
	val extenders: Vector[Extend] = Vector[Extend](),
	val matches: Vector[Match] = Vector[Match](),
	val groupings: Vector[Grouping] = Vector[Grouping](),
	val reducers: Vector[ReducerPackage] = Vector[ReducerPackage]()
) {

	def createAggregateIndexKey(reducer: Reducer): IndexKey = {
		IndexKey(reducer, extenders, matches, groupings)
	}
}