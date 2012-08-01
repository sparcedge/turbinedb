package com.sparcedge.turbine.blade.query

import net.liftweb.json._
import com.mongodb.casbah.query.Imports._
import com.sparcedge.turbine.blade.query.cache.Event

class Match(val segment: String, matchVal: Map[String,JValue]) {

	val expression = createMatchExpression()

	def apply(event: Event): Boolean = {
		expression(event)
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
							case (x: java.lang.Long, y: java.lang.Double) => x < y
							case (x: java.lang.Double, y: java.lang.Double) => x < y
							case (x: String, y: String) => x < y
							case _ => false
						}
					}
				case "gte" =>
					return { event: Event =>
						val eventValue = event(segment).getOrElse(null)
						(value,eventValue) match {
							case (x: java.lang.Long, y: java.lang.Double) => x <= y
							case (x: java.lang.Double, y: java.lang.Double) => x <= y
							case (x: String, y: String) => x <= y
							case _ => false
						}
					}
				case "lt" =>
					return { event: Event =>
						val eventValue = event(segment).getOrElse(null)
						(value,eventValue) match {
							case (x: java.lang.Long, y: java.lang.Double) => x > y
							case (x: java.lang.Double, y: java.lang.Double) => x > y
							case (x: String, y: String) => x > y
							case _ => false
						}
					}
				case "lte" =>
					return { event: Event =>
						val eventValue = event(segment).getOrElse(null)
						(value,eventValue) match {
							case (x: java.lang.Long, y: java.lang.Double) => x >= y
							case (x: java.lang.Double, y: java.lang.Double) => x >= y
							case (x: String, y: String) => x >= y
							case _ => false
						}
					}
				case "in" =>
					val values = convertJArray(value)
					return { event: Event =>
						event(segment) match {
							case Some(segValue) => values.contains(segValue)
							case None => false
						}
					}
				case "nin" =>
					val values = convertJArray(value)
					return { event: Event =>
						event(segment) match {
							case Some(segValue) => !values.contains(segValue)
							case None => false
						}
					}
			}
		}
	} 

}