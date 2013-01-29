package com.sparcedge.turbine.blade.query

import org.joda.time.format.DateTimeFormat
import com.sparcedge.turbine.blade.event.Event
import com.sparcedge.turbine.blade.util.CrazyDateUtil._

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

	def createGroup(event: Event): Any = {
		if(`type` == "duration") {
			val duration = value.get
			if(duration == "year") {
				calculateYearString(event.ts).toString
			} else if(duration == "month") {
				calculateMonthString(event.ts)
			} else if(duration == "day") {
				calculateDayString(event.ts)
			} else if(duration == "hour") {
				calculateHourString(event.ts)
			} else if(duration == "minute") {
				calculateMinuteString(event.ts)
			} else {
				throw new Exception("Invalid Duration Value")
			}
		} else if(`type` == "resource") {
			event("resource").getOrElse("").toString
		} else if(`type` == "segment") {
			event(value.get).getOrElse("").toString
		} else {
			throw new Exception("Bad Grouping Type")
		}
	}
}