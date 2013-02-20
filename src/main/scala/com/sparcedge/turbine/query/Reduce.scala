package com.sparcedge.turbine.query

import com.sparcedge.turbine.event.Event
import org.json4s.JsonAST._

class Reduce (reducers: Option[List[Reducer]], filter: Option[Map[String,JObject]]) {

	implicit val formats = org.json4s.DefaultFormats

	val filters = filter.getOrElse(Map[String,JValue]()) map { case (segment, value) => 
		new Match(segment, value.extract[Map[String,JValue]])
	}
	val reducerList = reducers.getOrElse(List[Reducer]())
}

case class Reducer (propertyName: String, reducer: String, segment: String) {
	def getCoreReducer(): CoreReducer = {
		CoreReducer(reducer, segment)
	}
}

case class CoreReducer (reducer: String, segment: String) {
	def createReducedResult(): ReducedResult = {
		reducer match {
			case "max" => new MaxReducedResult(segment)
			case "min" => new MinReducedResult(segment)
			case "avg" => new AvgReducedResult(segment)
			case "sum" => new SumReducedResult(segment)
			case "count" => new CountReducedResult(segment)
		}
	}
}