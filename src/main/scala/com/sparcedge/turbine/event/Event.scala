package com.sparcedge.turbine.event

case class Event (
	val its: Long, 
	val ts: Long, 
	val strValues: Map[String,String], 
	val dblValues: Map[String,Double]
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