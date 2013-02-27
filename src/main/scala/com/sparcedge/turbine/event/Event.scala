package com.sparcedge.turbine.event

import scala.collection.mutable

case class Event (
	val its: Long, 
	val ts: Long, 
	val strValues: mutable.Map[String,String], 
	val dblValues: mutable.Map[String,Double]
) {
	def apply(segment: String): Option[Any] = {
		dblValues.get(segment) orElse strValues.get(segment)	
	}

	def getDouble(segment: String): Option[Double] = {
		dblValues.get(segment)
	}

	def getString(segment: String): Option[String] = {
		strValues.get(segment)
	}

	def containsDouble(segment: String): Boolean = {
		dblValues.contains(segment)
	}

	def containsString(segment: String): Boolean = {
		strValues.contains(segment)
	}

	def getDoubleUnsafe(segment: String): Double = {
		dblValues(segment)
	}

	def getStringUnsafe(segment: String): String = {
		strValues(segment)
	}
}