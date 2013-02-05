package com.sparcedge.turbine.query

import com.sparcedge.turbine.event.Event
import org.json4s.JsonAST._

case class Match(val segment: String, matchVal: Map[String,JValue]) {

	val expression = createMatchExpression()

	def apply(event: Event): Boolean = {
		expression(event(segment).getOrElse(null))
	}

	def apply(): Boolean = {
		false
	}

	def apply(str: String): Boolean = {
		expression(str)
	}

	def apply(numeric: Double): Boolean = {
		expression(numeric)
	}

	def apply(ts: Long): Boolean = {
		expression(ts)
	}

	def unboxJValue(jval: JValue): Any = {
		jval match {
			case JString(jstr) => jstr
			case JInt(jint) => jint.toLong
			case JDouble(jdbl) => jdbl
			case JBool(jbl) => jbl
			case jarr: JArray => jarr
			case _ => None
		}
	}

	def convertJArray(maybeJarr: Any): List[Any] = {
		maybeJarr match {
			case JArray(jarr) => jarr.map(unboxJValue(_))
			case _ => List[Any]()
		}
	}

	def createMatchExpression(): Any => Boolean = {
		matchVal.head match { case(op, boxedVal) =>
			val value = unboxJValue(boxedVal)
			op match {
				case "eq" =>
					return { segVal => segVal == value }
				case "ne" =>
					return { segVal => segVal != value }
				case "gt" =>
					return { segVal => 
						(value,segVal) match {
							case (x: java.lang.Long, y: java.lang.Double) => x < y
							case (x: java.lang.Double, y: java.lang.Double) => x < y
							case (x: String, y: String) => x < y
							case _ => false
						}
					}
				case "gte" =>
					return { segVal =>
						(value,segVal) match {
							case (x: java.lang.Long, y: java.lang.Double) => x <= y
							case (x: java.lang.Double, y: java.lang.Double) => x <= y
							case (x: String, y: String) => x <= y
							case _ => false
						}
					}
				case "lt" =>
					return { segVal =>
						(value,segVal) match {
							case (x: java.lang.Long, y: java.lang.Double) => x > y
							case (x: java.lang.Double, y: java.lang.Double) => x > y
							case (x: String, y: String) => x > y
							case _ => false
						}
					}
				case "lte" =>
					return { segVal =>
						(value,segVal) match {
							case (x: java.lang.Long, y: java.lang.Double) => x >= y
							case (x: java.lang.Double, y: java.lang.Double) => x >= y
							case (x: String, y: String) => x >= y
							case _ => false
						}
					}
				case "in" =>
					val values: List[Any] = convertJArray(value)
					return { segVal => values.contains(segVal) }
				case "nin" =>
					val values: List[Any] = convertJArray(value)
					return { segVal => !values.contains(segVal) }
			}
		}
	}
}