package com.sparcedge.turbine.query

import org.joda.time.format.DateTimeFormat
import play.api.libs.json.Json
import play.api.libs.json.{JsObject,JsString}

import com.sparcedge.turbine.event.Event
import com.sparcedge.turbine.util.CrazierDateUtil._

object Grouping {
	def apply(jsObj: JsObject): Grouping = {
		jsObj.fields.head match {
			case ("segment", JsString(seg)) => new SegmentGrouping(seg)
			case ("duration", JsString(dur)) => new DurationGrouping(dur)
			case _ => throw new Exception("Invalid Grouping")
		}
	}
}

trait Grouping {
	val segment: String

	def apply(ts: Long, monthStart: Long): String = ts.toString
	def apply(ts: Long): String = ts.toString
	def apply(numeric: Double): String = numeric.toString
	def apply(str: String): String = str
	def apply(event: Event): String = event(segment).getOrElse("").toString
	val uniqueId: String
}

class SegmentGrouping(val segment: String) extends Grouping {
	val uniqueId: String = s"SegmentGrouping.${segment}"
}

class DurationGrouping(val duration: String) extends Grouping {
	val segment = "ts"

	override def apply(ts: Long): String = {
		duration match {
			case "year" => calculateYearCombined(ts).toString
			case "month" => calculateMonthCombined(ts).toString
			case "day" => calculateDayCombined(ts).toString
			case "hour" => calculateHourCombined(ts).toString
			case "minute" => calculateMinuteCombined(ts).toString
			case _ => throw new Exception("Invalid Duration Value")
		}
	}
	val uniqueId: String = s"DurationGrouping.${duration}"
}

class IndexGrouping(val indexDuration: String) extends Grouping {
	val segment = "ts"

	override def apply(ts: Long, monthStart: Long): String = {
		if(indexDuration == "hour") {
			(calculateAbsoluteHourForMonth(ts, monthStart) | 100000).toString
		} else if(indexDuration == "minute") {
			(calculateAbsoluteMinuteForMonth(ts, monthStart) | 100000).toString
		} else {
			throw new Exception("Invalid Index Duration Value")	
		}
	}
	val uniqueId: String = s"IndexGrouping.${indexDuration}"
}