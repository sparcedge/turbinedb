package com.sparcedge.turbine.blade.query

import net.liftweb.json._
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import com.mongodb.casbah.query.Imports._

object TurbineAnalyticsQuery {

	implicit val formats = Serialization.formats(NoTypeHints)

	def apply(queryStr: String): TurbineAnalyticsQuery = {
		val jsonObj = parse(queryStr)
		jsonObj.extract[TurbineAnalyticsQuery]
	}

}

case class TurbineAnalyticsQuery (
	blade: Blade,
	query: Query
) {
	def createCacheSegmentString(): String = {
		blade.domain + "." + blade.tenant + "." + blade.category + "." + blade.period
	}
}

case class Blade (
	domain: String,
	tenant: String,
	category: String,
	period: String
) {
	val formatter = DateTimeFormat.forPattern("yyyy-MM")
	val periodStart = formatter.parseDateTime(period)
	val periodEnd = periodStart.plusMonths(1)
}

case class Query (
	category: String,
	range: TimeRange,
	`match`: Option[Map[String,JValue]],
	group: Option[List[Grouping]],
	reduce: Option[Reduce]
) {
	// TODO: Update to retrieve all required fields not just matches
	def retrieveRequiredFields: Set[String] = {
		var reqFields = Set[String]()
		reqFields = reqFields ++ matches.map(_.segment)
		reqFields
	}

	implicit val formats = Serialization.formats(NoTypeHints)
	val matches = `match`.getOrElse(Map[String,JValue]()) map { case (segment, value) => 
		new Match(segment, value.extract[Map[String,JValue]])
	}
	lazy val requiredFields = retrieveRequiredFields
}

case class TimeRange (
	start: Long,
	end: Option[Long]
)

case class Grouping (
	duration: Option[String],
	resource: Option[Boolean],
	segment: Option[String]
)

case class Reduce (
	reducers: Option[List[Reducer]],
	filter: Option[Map[String,JObject]]
)

case class Reducer (
	propertyName: String,
	reducer: String,
	segment: String
)

class Match(val segment: String, matchVal: Map[String,JValue]) {
	val expression = createMatchExpression()

	def apply(event: Event): Boolean = {
		expression(event)
	}

	def unboxJValue(jval: JValue): Any = {
		jval match {
			case JString(jstr) => jstr
			case JInt(jint) => jint
			case JDouble(jdbl) => jdbl
			case JBool(jbl) => jbl
			case _ => None
		}
	}

	def createMatchExpression(): Event => Boolean = {
		matchVal.head match { case(op, boxedVal) =>
			val value = unboxJValue(boxedVal)
			op match {
				case "eq" =>
					return { event: Event =>
						event(segment) == value
					}
				case "ne" =>
					return { event: Event =>
						event(segment) != value
					}
				case "gt" =>
					return { event: Event =>
						val eventValue = event(segment)
						(value,eventValue) match {
							case (x: Int, y: Integer) =>
								x > y
							case (x: String, y: String) =>
								x > y
							case _ =>
								false
						}
					}
				case "gte" =>
					return { event: Event =>
						val eventValue = event(segment)
						(value,eventValue) match {
							case (x: Int, y: Integer) =>
								x > y
							case (x: String, y: String) =>
								x >= y
							case _ =>
								false
						}
					}
				case "lt" =>
					return { event: Event =>
						val eventValue = event(segment)
						(value,eventValue) match {
							case (x: Int, y: Integer) =>
								x > y
							case (x: String, y: String) =>
								x < y
							case _ =>
								false
						}
					}
				case "lte" =>
					return { event: Event =>
						val eventValue = event(segment)
						(value,eventValue) match {
							case (x: Int, y: Integer) =>
								x > y
							case (x: String, y: String) =>
								x <= y
							case _ =>
								false
						}
					}
				case "in" =>
					return { event: Event =>
						false
					}
				case "nin" =>
					return { event: Event =>
						false
					}
			}
		}
	} 

}