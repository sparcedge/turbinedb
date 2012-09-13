package com.sparcedge.turbine.blade.query

import org.joda.time.format.DateTimeFormat
import com.sparcedge.turbine.blade.event.Event

case class Grouping (`type`: String, value: Option[String]) {
	def createGroupFunction(): (Event) => Any = {
		`type` match {
			case "duration" =>
				val formatter = value.get match {
					case "year" =>
						DateTimeFormat.forPattern("yyyy")
					case "month" =>
						DateTimeFormat.forPattern("yyyy-MM")
					case "week" =>
						DateTimeFormat.forPattern("yyyy-ww")
					case "day" =>
						DateTimeFormat.forPattern("yyyy-MM-dd")
					case "hour" =>
						DateTimeFormat.forPattern("yyyy-MM-dd-HH")
					case "minute" =>
						DateTimeFormat.forPattern("yyyy-MM-dd-HH-mm")
					case _ =>
						throw new Exception("Invalid Duration Value")
				}
				{ event: Event => formatter.print(event.ts) }
			case "resource" =>
				{ event: Event => event("resource").getOrElse(null) }
			case "segment" =>
				{ event: Event => event(value.get).getOrElse(null) }
			case _ =>
				throw new Exception("Bad Grouping Type")
		}
	}

	lazy val groupFunction = createGroupFunction()
}