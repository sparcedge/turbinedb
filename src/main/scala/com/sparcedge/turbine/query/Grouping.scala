package com.sparcedge.turbine.query

import org.joda.time.format.DateTimeFormat
import play.api.libs.json.Json
import play.api.libs.json.{JsObject,JsString,JsValue,JsNumber}

import com.sparcedge.turbine.event.Event
import com.sparcedge.turbine.util.CrazierDateUtil._

object Grouping {
	def apply(segment: String): Grouping = {
		new Grouping(segment)
	}
}

class Grouping(segment: String) {
	val uniqueId = s"group.${segment}"

	def apply(numeric: Double): String = numeric.toString
	def apply(str: String): String = str
	def apply(event: Event): String = event(segment).getOrElse("").toString
}