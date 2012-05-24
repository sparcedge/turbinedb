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
	query: Query,
	qid: String
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
	def retrieveRequiredFields(): Set[String] = {
		var reqFields = Set[String]()
		reqFields = reqFields ++ matches.map(_.segment)
		reqFields = reqFields ++ groupings.filter(_.`type` == "segment").flatMap(_.value)
		reqFields = reqFields ++ reduce.map(_.reducerList map (_.segment)).getOrElse(List[String]())
		reqFields
	}

	implicit val formats = Serialization.formats(NoTypeHints)
	val matches = `match`.getOrElse(Map[String,JValue]()) map { case (segment, value) => 
		new Match(segment, value.extract[Map[String,JValue]])
	}
	lazy val requiredFields = retrieveRequiredFields()
	val groupings = group.getOrElse(List[Grouping]())
}

case class TimeRange (
	start: Long,
	end: Option[Long]
)

case class Grouping (
	`type`: String,
	value: Option[String]
) {
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
						DateTimeFormat.forPattern("yyyy-MM-dd-hh")
					case "minute" =>
						DateTimeFormat.forPattern("yyyy-MM-dd-hh-mm")
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

case class Reduce (
	reducers: Option[List[Reducer]],
	filter: Option[Map[String,JObject]]
) {
	implicit val formats = Serialization.formats(NoTypeHints)
	val filters = filter.getOrElse(Map[String,JValue]()) map { case (segment, value) => 
		new Match(segment, value.extract[Map[String,JValue]])
	}
	val reducerList = reducers.getOrElse(List[Reducer]())
}

case class Reducer (
	propertyName: String,
	reducer: String,
	segment: String
) {
	// Remove FlatMaps (Not Performant)
	def createReduceFunction(): (Iterable[Event]) => ((String,Any),(String,Any)) = {
	    reducer match {
			case "max" =>
				{ events: Iterable[Event] => 
					val numerics = events.flatMap(_(segment)).flatMap(convertNumeric(_))
					((propertyName,numerics.max),(propertyName + "-count",numerics.size))
				}
			case "min" =>
				{ events: Iterable[Event] => 
					val numerics = events.flatMap(_(segment)).flatMap(convertNumeric(_))
					((propertyName,numerics.min),(propertyName + "-count",numerics.size)) 
				}
			case "avg" => 
				{ events: Iterable[Event] => 
					val numerics = events.flatMap(_(segment)).flatMap(convertNumeric(_))
					val average = if (numerics.size > 0) numerics.sum / numerics.size else 0
					((propertyName,average),(propertyName + "-count",numerics.size))
				}
			case "sum" => 
				{ events: Iterable[Event] => 
					val numerics = events.flatMap(_(segment)).flatMap(convertNumeric(_))
					((propertyName,numerics.sum),(propertyName + "-count",numerics.size))
				}
			case "count" =>
				{ events: Iterable[Event] => 
					val properties = events.flatMap(_(segment))
					((propertyName,properties.size),(propertyName + "-count",properties.size))
				}
	    }
	}

	def convertNumeric(maybeNumeric: Any): Option[Double] = {
	    maybeNumeric match {
			case x: Int =>
				Some(x.toDouble)
			case x: Double =>
				Some(x)
			case x: Long =>
				Some(x.toDouble)
			case _ =>
				None
	    }
	}

	val reduceFunction = createReduceFunction()
}

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
						event(segment).getOrElse(null) == value
					}
				case "ne" =>
					return { event: Event =>
						event(segment).getOrElse(null) != value
					}
				case "gt" =>
					return { event: Event =>
						val eventValue = event(segment).getOrElse(null)
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
						val eventValue = event(segment).getOrElse(null)
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
						val eventValue = event(segment).getOrElse(null)
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
						val eventValue = event(segment).getOrElse(null)
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