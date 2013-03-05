package com.sparcedge.turbine.query

import org.joda.time.format.DateTimeFormat
import play.api.libs.json.Json
import play.api.libs.json.{JsObject,JsString,JsValue,JsNumber}

import com.sparcedge.turbine.event.Event
import com.sparcedge.turbine.util.CrazierDateUtil._

// Convert JsObject to Map....easier parse
object Grouping {
	def apply(jsObj: JsObject): Grouping = {
		retrieveGroupingValue(jsObj) match {
			case Some(("segment", JsString(seg))) => new SegmentGrouping(seg)
			case Some(("duration", JsString(dur))) => new DurationGrouping(dur, retrieveOffset(jsObj))
			case _ => throw new Exception("Invalid Grouping")
		}
	}

	def retrieveGroupingValue(jsObj: JsObject): Option[(String, JsValue)] = {
		jsObj.fields.find(f => f._1 == "segment" || f._1 == "duration")
	}

	def retrieveOffset(jsObj: JsObject): Option[Int] = {
		jsObj.fields.find(_._1 == "offset").map { field =>
			field match {
				case (_, JsNumber(num)) => num.toInt
				case _ => throw new Exception("Invalid offset value")
			}
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

class DurationGrouping(val duration: String, offsetOpt: Option[Int]) extends Grouping {
	val segment = "ts"
	val offset = offsetOpt.getOrElse(0)

	override def apply(event: Event): String = {
		apply(event.ts)
	}

	override def apply(ts: Long): String = {
		val tsOffset = applyGmtOffset(ts, offset)
		val durGroup = duration match {
			case "year" => calculateYearCombined(tsOffset)
			case "month" => calculateMonthCombined(tsOffset)
			case "day" => calculateDayCombined(tsOffset)
			case "hour" => calculateHourCombined(tsOffset)
			case "minute" => calculateMinuteCombined(tsOffset)
			case _ => throw new Exception("Invalid Duration Value")
		}
		durGroup.toString
	}

	val uniqueId: String = s"DurationGrouping.${duration}.${offset}"
}

class IndexGrouping(val indexDuration: String) extends Grouping {
	val segment = "ts"

	override def apply(ts: Long, monthStart: Long): String = {
		if(indexDuration == "hour") {
			calculateAbsoluteHourForMonth(ts, monthStart).toString
		} else if(indexDuration == "minute") {
			calculateAbsoluteMinuteForMonth(ts, monthStart).toString
		} else {
			throw new Exception("Invalid Index Duration Value")	
		}
	}
	val uniqueId: String = s"IndexGrouping.${indexDuration}"
}