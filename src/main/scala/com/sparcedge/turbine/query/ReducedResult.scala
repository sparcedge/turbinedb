package com.sparcedge.turbine.query

import com.sparcedge.turbine.event.Event

abstract class ReducedResult {
	val segment: String
	val reduceType: String

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

	def getResultValue(): Double

	def copyForOutput(out: String): OutputResult

	def reduce(newVal: Double)
	def reReduce(other: ReducedResult)
}

trait OutputResult extends ReducedResult {
	val output: String
}

class MaxReducedResult(val segment: String, var maximum: Double = 0.0, var initialized: Boolean = false) extends ReducedResult {
	val reduceType = "max"

	def reduce(newVal: Double) {
		if(newVal > maximum || !initialized) {
			maximum = newVal
			initialized = true
		}
	}

	def reReduce(other: ReducedResult) {
		if(other.isInstanceOf[MaxReducedResult]) {
			val maxResult = other.asInstanceOf[MaxReducedResult]
			if(!initialized) {
				maximum = maxResult.maximum
				initialized = maxResult.initialized
			} else if(maxResult.initialized && maxResult.maximum > maximum) {
				maximum = maxResult.maximum
				initialized = true
			}
		}
	}

	def getResultValue(): Double = maximum

	def copyForOutput(out: String): OutputResult = {
		new MaxReducedResult(segment, maximum, initialized) with OutputResult {
			val output = out
		}
	}
}

class MinReducedResult(val segment: String, var minimum: Double = 0.0, var initialized: Boolean = false) extends ReducedResult {
	val reduceType = "min"

	def reduce(newVal: Double) {
		if(newVal < minimum || !initialized) {
			minimum = newVal
			initialized = true
		}
	}

	def reReduce(other: ReducedResult) {
		if(other.isInstanceOf[MinReducedResult]) {
			val minResult = other.asInstanceOf[MinReducedResult]
			if(!initialized) {
				minimum = minResult.minimum
				initialized = minResult.initialized
			} else if(minResult.initialized && minResult.minimum < minimum) {
				minimum = minResult.minimum
				initialized = true
			}
		}		
	}

	def getResultValue(): Double = minimum

	def copyForOutput(out: String): OutputResult = {
		new MinReducedResult(segment, minimum, initialized) with OutputResult {
			val output = out
		}
	}
}

class AvgReducedResult(val segment: String, var sum: Double = 0.0, var count: Int = 0) extends ReducedResult {
	val reduceType = "avg"

	def reduce(newVal: Double) {
		sum += newVal
		count += 1
	}

	def reReduce(other: ReducedResult) {
		if(other.isInstanceOf[AvgReducedResult]) {
			val avgResult = other.asInstanceOf[AvgReducedResult]
			sum += avgResult.sum
			count += avgResult.count
		}
	}

	def getResultValue(): Double = {
		if(sum > 0) sum / count else 0
	}

	def copyForOutput(out: String): OutputResult = {
		new AvgReducedResult(segment, sum, count) with OutputResult {
			val output = out
		}
	}
}

class SumReducedResult(val segment: String, var sum: Double = 0.0) extends ReducedResult {
	val reduceType = "sum"

	def reduce(newVal: Double) {
		sum += newVal
	}

	def reReduce(other: ReducedResult) {
		if(other.isInstanceOf[SumReducedResult]) {
			val sumResult = other.asInstanceOf[SumReducedResult]
			sum += sumResult.sum
		}
	}

	def copyForOutput(out: String): OutputResult = {
		new SumReducedResult(segment, sum) with OutputResult {
			val output = out
		}
	}

	def getResultValue(): Double = sum
}

class CountReducedResult(val segment: String, var count: Int = 0) extends ReducedResult {
	val reduceType = "count"

	override def apply(event: Event) {
		val resOpt = event(segment)
		if(resOpt.isDefined) {
			count += 1
		}
	}

	def reduce(newVal: Double) {
		count += 1
	}

	def reReduce(other: ReducedResult) {
		if(other.isInstanceOf[ReducedResult]) {
			val countResult = other.asInstanceOf[CountReducedResult]
			count += countResult.count
		}
	}

	override def apply(str: String) {
		count += 1
	}

	def getResultValue(): Double = count

	def copyForOutput(out: String): OutputResult = {
		new CountReducedResult(segment, count) with OutputResult {
			val output = out
		}
	}
}

class StDevReducedResult(val segment: String, var diff: Double = 0.0, var mean: Double = 0.0, var count: Int = 0) extends ReducedResult {
	val reduceType = "stdev"

	def reduce(newVal: Double) {
		count += 1
		val delta = newVal - mean
		mean = mean + (delta / count)
		diff += delta * (newVal - mean)
	}

	// TODO: Encode Type Restrictions in trait/inheritance hierarchy
	def reReduce(other: ReducedResult) {
		if(other.isInstanceOf[StDevReducedResult]) {
			val stDev = other.asInstanceOf[StDevReducedResult]
			val tmpCnt = count
			val delta = mean - stDev.mean
			val weight = (count * stDev.count).toDouble / (count + stDev.count)
			diff += stDev.diff + (delta*delta*weight)
			count += stDev.count
			mean = ((mean*tmpCnt) + (stDev.mean*stDev.count)) / count
		}
	}

	def copyForOutput(out: String): OutputResult = {
		new StDevReducedResult(segment, diff, mean, count) with OutputResult {
			val output = out
		}
	}

	def getResultValue(): Double = {
		if(count > 0) Math.sqrt(diff / count) else 0
	}
}