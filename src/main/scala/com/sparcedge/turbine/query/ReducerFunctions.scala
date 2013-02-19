package com.sparcedge.turbine.query

import com.sparcedge.turbine.event.Event

object ReducerFunctions {

	def MAX_REREDUCE(res1: ReducedResult, res2: ReducedResult): (Double,Int) = {
		val max = if (res1.value > res2.value) res1.value else res2.value
		val count = res1.count + res2.count
		(max,count)
	}

	def MAX_STREAMING(prevValue: Double, count: Int, value: Double): (Double,Int) = {
		val newValue = if(value > prevValue) value else prevValue
		(newValue,count+1)
	}

	def MIN_REREDUCE(res1: ReducedResult, res2: ReducedResult): (Double,Int) = {
		val min = if (res1.value < res2.value) res1.value else res2.value
		val count = res1.count + res2.count
		(min,count)
	}

	def MIN_STREAMING(prevValue: Double, count: Int, value: Double): (Double,Int) = {
		val newValue = if(count == 0 || value < prevValue) value else prevValue
		(newValue,count+1)
	}

	def SUM_REREDUCE(res1: ReducedResult, res2: ReducedResult): (Double,Int) = {
		val sum = res1.value + res2.value
		val count = res1.count + res2.count
		(sum,count)
	}

	def SUM_STREAMING(prevValue: Double, count: Int, value: Double): (Double,Int) = {
		val newValue = value + prevValue
		(newValue, count+1)
	}

	def AVG_REREDUCE(res1: ReducedResult, res2: ReducedResult): (Double,Int) = {
		val sum = (res1.value * res1.count) + (res2.value * res2.count)
		val totalCount = res1.count + res2.count
		val newValue = if(totalCount > 0) sum / totalCount else 0
		(newValue,totalCount)
	}

	def AVG_STREAMING(prevValue: Double, count: Int, value: Double): (Double,Int) = {
		val newCount = count + 1
		val newValue = ((prevValue * count) + value) / newCount
		(newValue, newCount)
	}

	def COUNT_REREDUCE(res1: ReducedResult, res2: ReducedResult): (Double,Int) = {
		val count =  res1.count + res2.count
		(count,count)
	}

	def COUNT_STREAMING(prevValue: Double, count: Int, value: Double): (Double,Int) = {
		(prevValue+1,count+1)
	}
}