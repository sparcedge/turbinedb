package com.sparcedge.turbine.blade.query

import com.sparcedge.turbine.blade.query.cache.Event

object ReducerFunctions {

	val MAX = { (segment: String, events: Iterable[Event]) =>
		val numerics = retrieveNumericsForSegment(events, segment)
		val size = numerics.size
		val max = if(size > 0) numerics.max else 0
		ReducedResult(segment, max, size, "max")
	}

	val MAX_REREDUCE = { (results: Iterable[ReducedResult]) =>
		val max = results.map(_.value).max
		val count = results.map(_.count).sum
		ReducedResult(results.head.property, max, count, "max")
	}

	val MIN = { (segment: String, events: Iterable[Event]) =>
		val numerics = retrieveNumericsForSegment(events, segment)
		val size = numerics.size
		val min = if(size > 0) numerics.min else 0
		ReducedResult(segment, min, size, "min")
	}

	val MIN_REREDUCE = { (results: Iterable[ReducedResult]) =>
		val min = results.map(_.value).min
		val count = results.map(_.count).sum
		ReducedResult(results.head.property, min, count, "min")	
	}

	val SUM = { (segment: String, events: Iterable[Event]) =>
		val numerics = retrieveNumericsForSegment(events, segment)
		val size = numerics.size
		val sum = if(size > 0) numerics.sum else 0
		ReducedResult(segment, sum, size, "sum")
	}

	val SUM_REREDUCE = { (results: Iterable[ReducedResult]) =>
		val sum = results.map(_.value).sum
		val count = results.map(_.count).sum
		ReducedResult(results.head.property, sum, count, "sum")
	}

	val AVG = { (segment: String, events: Iterable[Event]) =>
		val numerics = retrieveNumericsForSegment(events, segment)
		val size = numerics.size
		val average = if (numerics.size > 0) numerics.sum / size else 0
		ReducedResult(segment, average, size, "avg")
	}

	val AVG_REREDUCE = { (results: Iterable[ReducedResult]) =>
		val sum = results.map(_.value).sum
		val count = results.map(_.count).sum
		ReducedResult(results.head.property, (sum / count), count, "avg")
	}

	val COUNT = { (segment: String, events: Iterable[Event]) =>
		val properties = unboxOptions(events.map(_(segment)))
		val size = properties.size
		ReducedResult(segment, size.toDouble, size, "count")
	}

	val COUNT_REREDUCE = { (results: Iterable[ReducedResult]) =>
		val count = results.map(_.count).sum
		ReducedResult(results.head.property, count, count, "count")
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