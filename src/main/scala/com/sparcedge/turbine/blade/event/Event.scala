package com.sparcedge.turbine.blade.event

trait Event {
	def ts: Long
	def strValues: Map[String,String]
	def dblValues: Map[String,Double]
	
	def apply(segment: String): Option[Any] = {
		dblValues.get(segment) orElse strValues.get(segment)	
	}

	def getDouble(segment: String): Option[Double] = {
		dblValues.get(segment)
	}

	def getString(segment: String): Option[String] = {
		strValues.get(segment)
	}
}