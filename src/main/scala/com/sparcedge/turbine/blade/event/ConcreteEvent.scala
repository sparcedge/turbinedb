package com.sparcedge.turbine.blade.event

import scala.collection.mutable
import com.mongodb.casbah.query.Imports._

object ConcreteEvent {
	def fromRawEvent(rawEvent: DBObject): ConcreteEvent = {
		val resource = rawEvent("r").toString
		val dataList = rawEvent("dat").asInstanceOf[DBObject].toList.+:("resource" -> resource)
		val sValues = mutable.Map[String,String]()
		val dValues = mutable.Map[String,Double]()

		dataList.foreach { case (key,value) =>
			value match {
				case x: java.lang.Long => dValues(key) = x.toDouble
				case x: java.lang.Integer => dValues(key) = x.toDouble
				case x: java.lang.Double => dValues(key) = x
				case x: String => sValues(key) = x
				case null => // Don't add to event
				case x => // TODO: Handle Exception Unknown Type
			}
		}

		val ts: Long = rawEvent("ts") match { 
			case x: java.lang.Long => x
			case x: java.lang.Double => x.toLong 
		}

		new ConcreteEvent(ts, sValues, dValues)
	}


}

class ConcreteEvent(val ts: Long, val strValues: mutable.Map[String,String], val dblValues: mutable.Map[String,Double]) extends Event