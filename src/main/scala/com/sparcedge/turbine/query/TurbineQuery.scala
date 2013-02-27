package com.sparcedge.turbine.query

import scala.util.{Try,Success,Failure}
import scala.collection.immutable.TreeMap
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.json4s._
import org.json4s.jackson.JsonMethods._

import com.sparcedge.turbine.data.IndexKey

object TurbineQueryPackage {

	implicit val formats = org.json4s.DefaultFormats

	def tryParse(queryStr: String): Try[TurbineQueryPackage] = {
		Try {
			val json = parse(queryStr)
			json.extract[TurbineQueryPackage]
		}
	}
}

case class TurbineQueryPackage(domain: String, tenant: String, category: String, query: TurbineQuery)

case class TurbineQuery (
	category: String,
	range: TimeRange,
	`match`: Option[Map[String,JValue]],
	group: Option[List[Grouping]],
	reduce: Option[Reduce]
) {
	implicit val formats = org.json4s.DefaultFormats
	val minuteFormatter = DateTimeFormat.forPattern("yyyy-MM-dd-HH-mm")
	val startPlusMinute = minuteFormatter.print(new DateTime(range.start).plusMinutes(1))
	val endMinute = range.end.map { end: Long => minuteFormatter.print(new DateTime(end)) }
	val orderedMatches = `match`.map(unorderedMatches => TreeMap(unorderedMatches.toArray:_*))

	def createAggregateCacheString(reducer: Reducer): String = {
		orderedMatches.mkString + "-" + group.mkString + "-" + reducer.segment + "-" + reducer.reducer
	}

	def createAggregateIndexKey(reducer: Reducer): IndexKey = {
		IndexKey(reducer.getCoreReducer(), matches, groupings)
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
	val reducers = reduce match {
		case Some(reduce) => reduce.reducerList
		case None => List[Reducer]()
	}
	val groupings = group.getOrElse(List[Grouping]())
}

case class TimeRange (start: Long, end: Option[Long])