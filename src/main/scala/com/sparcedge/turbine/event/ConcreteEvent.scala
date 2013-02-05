package com.sparcedge.turbine.event

import scala.collection.mutable

object ConcreteEvent {
	def convertTimestamp(obj: Any): Long = {
		obj match {
			case x: java.lang.Long => x
			case x: java.lang.Double => x.toLong
		}
	}
}

class ConcreteEvent(val its: Long, val ts: Long, val strValues: mutable.Map[String,String], val dblValues: mutable.Map[String,Double]) extends Event