package com.sparcedge.turbine.query

import com.sparcedge.turbine.event.Event
import com.sparcedge.turbine.data.{SegmentValueHolder,DataTypes}

abstract class ReducedResult(val segment: String) {
	val reduceType: String
	var segmentPlaceholder = SegmentValueHolder(segment)

	def execute() = (segmentPlaceholder.getType) match {
        case DataTypes.NUMBER => reduce(segmentPlaceholder.getDouble)
        case DataTypes.STRING => reduce(segmentPlaceholder.getString)
        case DataTypes.TIMESTAMP => reduce(segmentPlaceholder.getTimestamp.toDouble)
        case _ => // Do Nothing
    }

	def apply(placeholder: SegmentValueHolder) {
		if(placeholder.segment == segment) {
			segmentPlaceholder = placeholder
		}
	}

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

	def reduce(newVal: String) { /* Do Nothing By Default */ }

	def reReduce(other: ReducedResult)
}

trait OutputResult extends ReducedResult {
	val output: String
}

class MaxReducedResult(seg: String, var maximum: Double = 0.0, var initialized: Boolean = false) extends ReducedResult(seg) {
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

class MinReducedResult(seg: String, var minimum: Double = 0.0, var initialized: Boolean = false) extends ReducedResult(seg) {
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

class AvgReducedResult(seg: String, var sum: Double = 0.0, var count: Int = 0) extends ReducedResult(seg) {
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
		if(count <= 0) 0 else sum / count
	}

	def copyForOutput(out: String): OutputResult = {
		new AvgReducedResult(segment, sum, count) with OutputResult {
			val output = out
		}
	}
}

class SumReducedResult(seg: String, var sum: Double = 0.0) extends ReducedResult(seg) {
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

class CountReducedResult(seg: String, var count: Int = 0) extends ReducedResult(seg) {
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

class StDevReducedResult(seg: String, var diff: Double = 0.0, var mean: Double = 0.0, var count: Int = 0) extends ReducedResult(seg) {
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

class RangeReducedResult(seg: String, var maximum: Double = 0.0, var minimum: Double = 0.0, var initialized: Boolean = false) extends ReducedResult(seg) {
	val reduceType = "range"

	def reduce(newVal: Double) {
		if(!initialized) {
			maximum = newVal
			minimum = newVal
			initialized = true
		} else if(newVal > maximum) {
			maximum = newVal
		} else if(newVal < minimum) {
			minimum = newVal
		}
	}

	def reReduce(other: ReducedResult) {
		if(other.isInstanceOf[RangeReducedResult]) {
			val rngResult = other.asInstanceOf[RangeReducedResult]
			if(!initialized) {
				maximum = rngResult.maximum
				minimum = rngResult.minimum
				initialized = rngResult.initialized
			} else if(rngResult.initialized && rngResult.maximum > maximum) {
				maximum = rngResult.maximum
				initialized = true
			} else if(rngResult.initialized && rngResult.minimum < minimum) {
				minimum = rngResult.minimum
				initialized = true
			}
		}
	}

	def getResultValue(): Double = {
		maximum - minimum
	}

	def copyForOutput(out: String): OutputResult = {
		new RangeReducedResult(segment, maximum, minimum) with OutputResult {
			val output = out
		}
	}
}

class VarianceReducedResult(seg: String, var diff: Double = 0.0, var mean: Double = 0.0, var count: Int = 0) extends ReducedResult(seg) {
	val reduceType = "variance"

	def reduce(newVal: Double) {
		count += 1
		val delta = newVal - mean
		mean = mean + (delta / count)
		diff += delta * (newVal - mean)
	}

	// TODO: Encode Type Restrictions in trait/inheritance hierarchy
	def reReduce(other: ReducedResult) {
		if(other.isInstanceOf[VarianceReducedResult]) {
			val variance = other.asInstanceOf[VarianceReducedResult]
			val tmpCnt = count
			val delta = mean - variance.mean
			val weight = (count * variance.count).toDouble / (count + variance.count)
			diff += variance.diff + (delta*delta*weight)
			count += variance.count
			mean = ((mean*tmpCnt) + (variance.mean*variance.count)) / count
		}
	}

	def copyForOutput(out: String): OutputResult = {
		new VarianceReducedResult(segment, diff, mean, count) with OutputResult {
			val output = out
		}
	}

	def getResultValue(): Double = {
		if(count > 0) diff / count else 0
	}
}