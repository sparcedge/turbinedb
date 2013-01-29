package com.sparcedge.turbine.blade.query

import com.sparcedge.turbine.blade.event.Event
import org.json4s.JsonAST._

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

	implicit val formats = org.json4s.DefaultFormats

	val filters = filter.getOrElse(Map[String,JValue]()) map { case (segment, value) => 
		new Match(segment, value.extract[Map[String,JValue]])
	}
	val reducerList = reducers.getOrElse(List[Reducer]())
}

case class Reducer (val propertyName: String, val reducer: String, val segment: String) {

	def createReduceFunction(): (Iterable[Event]) => ReducedResult = {
	    reducer match {
			case "max" => ReducerFunctions.MAX(segment, _:Iterable[Event])
			case "min" => ReducerFunctions.MIN(segment, _:Iterable[Event])
			case "avg" => ReducerFunctions.AVG(segment, _:Iterable[Event])
			case "sum" => ReducerFunctions.SUM(segment, _:Iterable[Event])
			case "count" => ReducerFunctions.COUNT(segment, _:Iterable[Event])
	    }
	}

	def createReducedResult(): ReducedResult = {
		new ReducedResult(segment, reducer, Some(propertyName))
	}

	val reduceFunction = createReduceFunction()
}

class ReducedResult (val segment: String, val reducer: String, var output: Option[String], var value: Double = 0.0, var count: Int = 0) {

	val streamingReduceFunction = reducer match {
		case "max" => ReducerFunctions.MAX_STREAMING(_:Double, _:Int, _:Double)
		case "min" => ReducerFunctions.MIN_STREAMING(_:Double, _:Int, _:Double)
		case "avg" => ReducerFunctions.AVG_STREAMING(_:Double, _:Int, _:Double)
		case "sum" => ReducerFunctions.SUM_STREAMING(_:Double, _:Int, _:Double)
		// TODO: Make String Work!
		case "count" => ReducerFunctions.COUNT_STREAMING(_:Double, _:Int, _:Double)
	}

	val reReduceFunction = reducer match {
		case "max" => ReducerFunctions.MAX_REREDUCE(this, _:ReducedResult)
		case "min" => ReducerFunctions.MIN_REREDUCE(this, _:ReducedResult)
		case "avg" => ReducerFunctions.AVG_REREDUCE(this, _:ReducedResult)
		case "sum" => ReducerFunctions.SUM_REREDUCE(this, _:ReducedResult)
		case "count" => ReducerFunctions.COUNT_REREDUCE(this, _:ReducedResult)
	}

	def apply(event: Event) {
		val dblOpt = event.getDouble(segment)
		if(!dblOpt.isEmpty) {
			val (newValue,newCount) = streamingReduceFunction(value, count, dblOpt.get)
			value = newValue
			count = newCount
		}
	}

	def reReduce(other: ReducedResult): ReducedResult = {
		reReduceFunction(other)
	}

	def createOutputResult(out: String): ReducedResult = {
		new ReducedResult(segment, reducer, Some(out), value, count)
	}
}