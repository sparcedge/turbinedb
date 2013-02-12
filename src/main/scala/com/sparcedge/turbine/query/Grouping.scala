package com.sparcedge.turbine.query

import org.joda.time.format.DateTimeFormat
import com.sparcedge.turbine.event.Event
import com.sparcedge.turbine.util.CrazierDateUtil._

case class Grouping (`type`: String, value: Option[String]) {

	val requiredSegment: String = `type` match {
		case "duration" => "ts"
		case "resource" => "resource"
		case "segment" => value.get
	}

	val segment: Option[String] = `type` match {
		case "duration" => None
		case "resource" => Some("resource")
		case "segment" => Some(value.get)
	}

	def apply(ts: Long, monthStart: Long): String = {
		val duration = value.get
		if(duration == "ihour") {
			(calculateAbsoluteHourForMonth(ts, monthStart) % 100000).toString
		} else if(duration == "iminute") {
			(calculateAbsoluteMinuteForMonth(ts, monthStart) % 100000).toString
		} else {
			throw new Exception("Invalid Duration Value")	
		}
	}

	def apply(ts: Long): String = {
		if(`type` != "duration") {
			throw new Exception(s"Only use timestamp with duration grouping! Found: ${`type`}")
		}
		val duration = value.get
		convertDuration(ts, duration)
	}

	def apply(numeric: Double): String = {
		numeric.toString
	}

	def apply(str: String): String = {
		str
	}

	def convertDuration(ts: Long, duration: String): String = {
		if(duration == "year") {
			calculateYearCombined(ts).toString
		} else if(duration == "month") {
			calculateMonthCombined(ts).toString
		} else if(duration == "day") {
			calculateDayCombined(ts).toString
		} else if(duration == "hour") {
			calculateHourCombined(ts).toString
		} else if(duration == "minute") {
			calculateMinuteCombined(ts).toString
		} else {
			throw new Exception("Invalid Duration Value")
		}
	}

	def createGroup(event: Event): String = {
		if(`type` == "duration") {
			val duration = value.get
			convertDuration(event.ts, duration)
		} else if(`type` == "resource") {
			event("resource").getOrElse("").toString
		} else if(`type` == "segment") {
			event(value.get).getOrElse("").toString
		} else {
			throw new Exception("Bad Grouping Type")
		}
	}
}