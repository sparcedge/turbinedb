package com.sparcedge.turbine.blade.query

import net.liftweb.json._
import com.mongodb.casbah.query.Imports._
import com.sparcedge.turbine.blade.query.cache.Event

object Reduce {
	def reReduce(results: Iterable[ReducedResult]): ReducedResult = {
		results.head.reducer match {
			case "max" => ReducerFunctions.MAX_REREDUCE(results)
			case "min" => ReducerFunctions.MIN_REREDUCE(results)
			case "avg" => ReducerFunctions.AVG_REREDUCE(results)
			case "sum" => ReducerFunctions.SUM_REREDUCE(results)
			case "count" => ReducerFunctions.COUNT_REREDUCE(results)
		}
	}
}

class Reduce (reducers: Option[List[Reducer]], filter: Option[Map[String,JObject]]) {

	implicit val formats = Serialization.formats(NoTypeHints)
	val filters = filter.getOrElse(Map[String,JValue]()) map { case (segment, value) => 
		new Match(segment, value.extract[Map[String,JValue]])
	}
	val reducerList = reducers.getOrElse(List[Reducer]())
}

class Reducer (val propertyName: String, val reducer: String, val segment: String) {

	def createReduceFunction(): (Iterable[Event]) => ReducedResult = {
	    reducer match {
			case "max" => ReducerFunctions.MAX(segment, _:Iterable[Event])
			case "min" => ReducerFunctions.MIN(segment, _:Iterable[Event])
			case "avg" => ReducerFunctions.AVG(segment, _:Iterable[Event])
			case "sum" => ReducerFunctions.SUM(segment, _:Iterable[Event])
			case "count" => ReducerFunctions.COUNT(segment, _:Iterable[Event])
	    }
	}

	def convertNumeric(maybeNumeric: Any): Option[Double] = {
	    maybeNumeric match {
			case x: Int =>
				Some(x.toDouble)
			case x: Double =>
				Some(x)
			case x: Long =>
				Some(x.toDouble)
			case _ =>
				None
	    }
	}

	val reduceFunction = createReduceFunction()
}

case class ReducedResult ( property: String, value: Double, count: Int, reducer: String, var output: Option[String] = None)