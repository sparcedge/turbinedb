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
			case Some(("duration", JsString(dur))) => DurationGrouping(dur, retrieveOffset(jsObj))
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

object DurationGrouping {
	def apply(duration: String, offsetOpt: Option[Int]): DurationGrouping = {
		duration match {
			case "year" => new YearDurationGrouping(offsetOpt)
			case "month" => new MonthDurationGrouping(offsetOpt)
			case "day" => new DayDurationGrouping(offsetOpt)
			case "hour" => new HourDurationGrouping(offsetOpt)
			case "minute" => new MinuteDurationGrouping(offsetOpt)
			case _ => throw new Exception("Invalid Duration Value")
		}
	}
}

trait DurationGrouping extends Grouping {
	val duration: String
	val segment = "ts"
	val offset: Int

	override def apply(event: Event): String = {
		apply(event.ts)
	}

	override def apply(ts: Long): String = {
		val tsOffset = applyGmtOffset(ts, offset)
		calculateDurationValue(tsOffset).toString
	}

	def calculateDurationValue(ts: Long): Long

	val uniqueId: String = s"DurationGrouping.${duration}.${offset}"
}

class YearDurationGrouping(offsetOpt: Option[Int]) extends DurationGrouping {
	val duration = "year"
	val offset = offsetOpt.getOrElse(0)

	def calculateDurationValue(ts: Long): Long = calculateYearCombined(ts)
}

class MonthDurationGrouping(offsetOpt: Option[Int]) extends DurationGrouping {
	val duration = "month"
	val offset = offsetOpt.getOrElse(0)

	def calculateDurationValue(ts: Long): Long = calculateMonthCombined(ts)
}

class DayDurationGrouping(offsetOpt: Option[Int]) extends DurationGrouping {
	val duration = "day"
	val offset = offsetOpt.getOrElse(0)

	def calculateDurationValue(ts: Long): Long = calculateDayCombined(ts)
}

class HourDurationGrouping(offsetOpt: Option[Int]) extends DurationGrouping {
	val duration = "hour"
	val offset = offsetOpt.getOrElse(0)

	def calculateDurationValue(ts: Long): Long = calculateHourCombined(ts)
}

class MinuteDurationGrouping(offsetOpt: Option[Int]) extends DurationGrouping {
	val duration = "minute"
	val offset = offsetOpt.getOrElse(0)

	def calculateDurationValue(ts: Long): Long = calculateMinuteCombined(ts)
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