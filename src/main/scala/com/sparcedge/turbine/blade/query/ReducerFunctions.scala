package com.sparcedge.turbine.blade.query

import com.sparcedge.turbine.blade.query.cache.Event

object ReducerFunctions {

	def MAX(segment: String, events: Iterable[Event]): ReducedResult = {
		val numerics = retrieveNumericsForSegment(events, segment)
		val size = numerics.size
		val max = if(size > 0) numerics.max else 0
		new ReducedResult(segment, "max", None, max, size)
	}

	def MAX_REREDUCE(results: Iterable[ReducedResult]): ReducedResult = {
		val max = results.map(_.value).max
		val count = results.map(_.count).sum
		new ReducedResult(results.head.segment, "max", None, max, count)
	}

	def MAX_STREAMING(prevValue: Double, count: Int, maybeValue: Option[Any]): (Double,Int) = {
		convertNumeric(maybeValue) match {
			case Some(value) => 
				val newValue = if(value > prevValue) value else prevValue
				(newValue,count+1)
			case None => 
				(prevValue,count)
		}
	}

	def MIN(segment: String, events: Iterable[Event]): ReducedResult = {
		val numerics = retrieveNumericsForSegment(events, segment)
		val size = numerics.size
		val min = if(size > 0) numerics.min else 0
		new ReducedResult(segment, "min", None, min, size)
	}

	def MIN_REREDUCE(results: Iterable[ReducedResult]): ReducedResult = {
		val min = results.map(_.value).min
		val count = results.map(_.count).sum
		new ReducedResult(results.head.segment, "min", None, min, count)	
	}

	def MIN_STREAMING(prevValue: Double, count: Int, maybeValue: Option[Any]): (Double,Int) = {
		convertNumeric(maybeValue) match {
			case Some(value) => 
				val newValue = if(value < prevValue) value else prevValue
				(newValue, count+1)
			case None => 
				(prevValue, count)
		}
	}

	def SUM(segment: String, events: Iterable[Event]): ReducedResult = {
		val numerics = retrieveNumericsForSegment(events, segment)
		val size = numerics.size
		val sum = if(size > 0) numerics.sum else 0
		new ReducedResult(segment, "sum", None, sum, size)
	}

	def SUM_REREDUCE(results: Iterable[ReducedResult]): ReducedResult = {
		val sum = results.map(_.value).sum
		val count = results.map(_.count).sum
		new ReducedResult(results.head.segment, "sum", None, sum, count)
	}

	def SUM_STREAMING(prevValue: Double, count: Int, maybeValue: Option[Any]): (Double,Int) = {
		convertNumeric(maybeValue) match {
			case Some(value) => 
				val newValue = value + prevValue
				(newValue, count+1)
			case None => 
				(prevValue, count)
		}
	}

	def AVG(segment: String, events: Iterable[Event]): ReducedResult = {
		val numerics = retrieveNumericsForSegment(events, segment)
		val size = numerics.size
		val average = if (numerics.size > 0) numerics.sum / size else 0
		new ReducedResult(segment, "avg", None, average, size)
	}

	def AVG_REREDUCE(results: Iterable[ReducedResult]): ReducedResult = {
		val sum = results.map(_.value).sum
		val count = results.size
		val totalCount = results.map(_.count).sum
		new ReducedResult(results.head.segment, "avg", None, (sum / count), totalCount)
	}

	def AVG_STREAMING(prevValue: Double, count: Int, maybeValue: Option[Any]): (Double,Int) = {
		convertNumeric(maybeValue) match {
			case Some(value) => 
				val newCount = count + 1
				val newValue = (value + prevValue) / newCount
				(newValue, newCount)
			case None => 
				(prevValue, count)
		}
	}

	def COUNT(segment: String, events: Iterable[Event]): ReducedResult = {
		val properties = unboxOptions(events.map(_(segment)))
		val size = properties.size
		new ReducedResult(segment, "count", None, size.toDouble, size)
	}

	def COUNT_REREDUCE(results: Iterable[ReducedResult]): ReducedResult = {
		val count = results.map(_.count).sum
		new ReducedResult(results.head.segment, "count", None, count, count)
	}

	def COUNT_STREAMING(prevValue: Double, count: Int, maybeValue: Option[Any]): (Double,Int) = {
		maybeValue match {
			case Some(value) => (prevValue+1,count+1)
			case None => (prevValue, count)
		}
	}

	def retrieveNumericsForSegment(events: Iterable[Event], segment: String): Iterable[Double] = {
		unboxOptions(events.map(_(segment)).map(convertNumeric(_)))
	}

	def unboxOptions[T<:Any](options: Iterable[Option[T]]): Iterable[T] = {
		options.filter(_ != None).map(_.get)
	}

	def convertNumeric(maybeNumeric: Option[Any]): Option[Double] = {
	    maybeNumeric match {
			case Some(x: Int) =>
				Some(x.toDouble)
			case Some(x: Double) =>
				Some(x)
			case Some(x: Long) =>
				Some(x.toDouble)
			case _ =>
				None
	    }
	}

}