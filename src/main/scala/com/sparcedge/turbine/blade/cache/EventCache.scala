package com.sparcedge.turbine.blade.cache

import scala.concurrent.ExecutionContext
import com.sparcedge.turbine.blade.query._
import com.sparcedge.turbine.blade.util.{Timer,WrappedTreeMap}
import org.joda.time.format.DateTimeFormat

object EventCache {

	def apply(blade: Blade): EventCache = {
		val diskCache = new DiskCache(blade)
		new EventCache(blade, diskCache)
	}
}

class EventCache(val blade: Blade, val diskCache: DiskCache) {
	val periodStart = blade.periodStart.getMillis
	val periodEnd = blade.periodEnd.getMillis
	val aggregateCache = new AggregateCache(this)

	// Currently only working with groupings / reducers
	def applyQuery(query: TurbineQuery)(implicit ec: ExecutionContext): String = {
		val timer = new Timer()
		var json = ""

		timer.start()
		val aggregateResults = calculateAggregateResultsFromCache(query.query)
		timer.stop("Query Processing")
		timer.start()
		json = CustomJsonSerializer.serializeAggregateGroupMap(aggregateResults)
		timer.stop("Serialize Results")

		json
	}

	def calculateAggregateResultsFromCache(query: Query)(implicit ec: ExecutionContext): WrappedTreeMap[String,List[ReducedResult]] = {
		aggregateCache.calculateQueryResults(query)
	}
}