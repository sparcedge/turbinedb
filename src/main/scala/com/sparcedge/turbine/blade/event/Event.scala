package com.sparcedge.turbine.blade.event

import scala.collection.mutable

trait Event {
	def its: Long
	def ts: Long
	def strValues: mutable.Map[String,String]
	def dblValues: mutable.Map[String,Double]
	
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