package com.sparcedge.turbine.query

import play.api.libs.json.Json
import play.api.libs.json.{JsObject,JsString}

import com.sparcedge.turbine.event.Event

object Reducer {
	def apply(jsObj: JsObject): Reducer = {
		jsObj.fields.head match {
			case (propertyName: String, reducerObj: JsObject) =>
				createReducer(propertyName, reducerObj)
			case _ =>
				throw new Exception("Invalid Reducer")
		}
	}

	def createReducer(propertyName: String, reducerObj: JsObject): Reducer = {
		reducerObj.fields.head match {
			case (reducer: String, JsString(segment)) => 
				new Reducer(propertyName, reducer, segment)
			case _ =>
				throw new Exception("Invalid Reducer")
		}
		
	}
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
			case "stdev" => new StandardDeviationReducedResult(segment)
		}
	}
}