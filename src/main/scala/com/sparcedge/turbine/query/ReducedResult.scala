package com.sparcedge.turbine.query

import com.sparcedge.turbine.event.Event

abstract class ReducedResult {
	val segment: String
	var value: Double
	var count: Int
	val reducer: String

	def apply(event: Event) {
		val dblOpt = event.getDouble(segment)
		if(dblOpt.isDefined) {
			reduce(dblOpt.get)
		}
	}

	def apply(numeric: Double) {
		reduce(numeric)
	}

	def apply(str: String) {
		// Do Nothing (Can be overridden)
	}

	def apply(reducedResult: ReducedResult) {
		reReduce(reducedResult)
	}

	def getResultValue(): Double = {
		value
	}

	def copyForOutput(out: String): OutputResult

	def reduce(newVal: Double)
	def reReduce(other: ReducedResult)
}

trait OutputResult extends ReducedResult {
	val output: String
}

class MaxReducedResult(val segment: String, var value: Double = 0.0, var count: Int = 0) extends ReducedResult {
	val reducer = "max"

	def reduce(newVal: Double) {
		if(newVal > value) {
			value = newVal
		}
		count +=1
	}

	def reReduce(other: ReducedResult) {
		if(other.value > value) {
			value = other.value
		}
		count += other.count
	}

	def copyForOutput(out: String): OutputResult = {
		new MaxReducedResult(segment,value,count) with OutputResult {
			val output = out
		}
	}
}

class MinReducedResult(val segment: String, var value: Double = 0.0, var count: Int = 0) extends ReducedResult {
	val reducer = "min"

	def reduce(newVal: Double) {
		if(newVal < value) {
			value = newVal
		}
		count += 1
	}

	def reReduce(other: ReducedResult) {
		if(other.value < value) {
			value = other.value
		}
		count += other.count
	}

	def copyForOutput(out: String): OutputResult = {
		new MinReducedResult(segment,value,count) with OutputResult {
			val output = out
		}
	}
}

class AvgReducedResult(val segment: String, var value: Double = 0.0, var count: Int = 0) extends ReducedResult {
	val reducer = "avg"

	def reduce(newVal: Double) {
		value += newVal
		count += 1
	}

	def reReduce(other: ReducedResult) {
		value += other.value
		count += other.count
	}

	override def getResultValue(): Double = {
		if(count > 0) value / count else 0
	}

	def copyForOutput(out: String): OutputResult = {
		new AvgReducedResult(segment,value,count) with OutputResult {
			val output = out
		}
	}
}

class SumReducedResult(val segment: String, var value: Double = 0.0, var count: Int = 0) extends ReducedResult {
	val reducer = "sum"

	def reduce(newVal: Double) {
		value += newVal
		count += 1
	}

	def reReduce(other: ReducedResult) {
		value += other.value
		count += other.count
	}

	def copyForOutput(out: String): OutputResult = {
		new SumReducedResult(segment,value,count) with OutputResult {
			val output = out
		}
	}
}

class CountReducedResult(val segment: String, var value: Double = 0.0, var count: Int = 0) extends ReducedResult {
	val reducer = "count"

	def reduce(newVal: Double) {
		value += 1
		count += 1
	}

	def reReduce(other: ReducedResult) {
		value += other.value
		count += other.count
	}

	override def apply(str: String) {
		value += 1
		count += 1
	}

	def copyForOutput(out: String): OutputResult = {
		new CountReducedResult(segment,value,count) with OutputResult {
			val output = out
		}
	}
}

class StandardDeviationReducedResult(val segment: String, var value: Double = 0.0, var count: Int = 0) extends ReducedResult {
	val reducer = "stddev"
	var m = 0.0
	var s = 0.0
	var k = 1

	def reduce(newVal: Double) {
		var tmpM = m
		m += (newVal - tmpM) / k
		s += (newVal - tmpM) * (newVal - m)
		k += 1
		count += 1
	}

	// TODO: Encode Type Restrices in trait/inheritance hierarchy
	// TODO: Bryan made reReduce algorithm so probably wrong
	def reReduce(other: ReducedResult) {
		if(other.isInstanceOf[StandardDeviationReducedResult]) {
			val stDev = other.asInstanceOf[StandardDeviationReducedResult]
			val tmpk = k + stDev.k
			m = ((m*k)+(stDev.m*stDev.k)) / tmpk
			s = ((s*k)+(stDev.s*stDev.k)) / tmpk
			k = tmpk / 2
		}
	}

	def copyForOutput(out: String): OutputResult = {
		val stDevOut = new StandardDeviationReducedResult(segment, value, count) with OutputResult {
			val output = out
		}
		stDevOut.m = m
		stDevOut.s = s
		stDevOut.k = k
		stDevOut
	}

	override def getResultValue(): Double = {
		Math.sqrt(s / (k-1))
	}
}