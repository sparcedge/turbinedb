package com.sparcedge.turbine.blade.query.cache

import scala.collection.mutable
import scala.collection.immutable.TreeMap
import com.sparcedge.turbine.blade.query._
import akka.dispatch.{Await,Future,ExecutionContext}
import akka.util.duration._

class AggregateCache(cache: EventCache) {

	val aggregateCache = mutable.Map[String,Future[CachedAggregate]]()
	val hourGrouping = Grouping("duration", Some("hour"))

	// TODO: Clean up tuple mess
	def calculateQueryResults(query: Query)(implicit ec: ExecutionContext): TreeMap[String,Iterable[ReducedResult]] = {
		var startTime = System.currentTimeMillis
		val aggregates = retrieveCachedAggregatesForQuery(query)
		var endTime = System.currentTimeMillis
		println("Create/Retrieve Cached Aggregates: " + (endTime - startTime))

		startTime = System.currentTimeMillis
		val updatedAggregates = aggregates.map { case (property,aggregate) => (property,sliceAndMergeBoundaryData(query, aggregate)) }
		endTime = System.currentTimeMillis
		println("Slice/Merge Boundary Data: " + (endTime - startTime))

		startTime = System.currentTimeMillis
		val reducedAggregates = updatedAggregates.map { case (property,aggregate) => QueryResolver.removeHourGroupFlattendAndReduceAggregate(aggregate, property) }
		endTime = System.currentTimeMillis
		println("Flatten/Re-reduce Aggregates: " + (endTime - startTime))

		startTime = System.currentTimeMillis
		val flattened = QueryResolver.flattenAggregates(reducedAggregates)
		endTime = System.currentTimeMillis
		println("Flatten Grouped Aggregates: " + (endTime - startTime))

		flattened
	}

	def retrieveCachedAggregatesForQuery(query: Query)(implicit ec: ExecutionContext): List[(String,CachedAggregate)] = {
		val reducers = query.reduce match {
			case Some(reduce) => reduce.reducerList
			case None => List[Reducer]()
		}
		// TODO: Timeout / Error Handling
		val propertyReduceMap = reducers.map { reducer => (reducer.propertyName, retrieveAndOptionallyAddCachedAggregate(query, reducer)) }
		propertyReduceMap.map { case (property,aggFuture) => (property, Await.result(aggFuture, 10 seconds)) }
	}

	private def retrieveAndOptionallyAddCachedAggregate(query: Query, reducer: Reducer)(implicit ec: ExecutionContext): Future[CachedAggregate] = {
		val aggregateCacheString = query.createAggregateCacheString(reducer)
		val futureAggregate = aggregateCache.getOrElseUpdate(aggregateCacheString, {
			Future {
				caclculateAggregate(query, reducer)
			}
		})
		futureAggregate
	}

	// TODO: Handle case with no groupings
	private def caclculateAggregate(query: Query, reducer: Reducer): CachedAggregate = {
		val matchedEvents = QueryResolver.applyMatches(cache.events, query.matches)
		println(matchedEvents.size)
		val eventGroupings = QueryResolver.applyGroupings(matchedEvents, hourGrouping :: query.groupings)
		val aggregates = QueryResolver.applyReducerToEventGroupings(eventGroupings, reducer)
		new CachedAggregate (
			matchSet = query.matches,
			groupSet = query.groupings,
			reducer = reducer,
			aggregateMap = aggregates
		)
	}

	def sliceAndMergeBoundaryData(query: Query, aggregate: CachedAggregate): TreeMap[String,ReducedResult] = {
		val lowerBoundBroken = query.range.start > cache.periodStart
		var upperBoundBroken = query.range.end != None && query.range.end.get < cache.periodEnd

		val slicedData = sliceAggregate(query, aggregate, lowerBoundBroken, upperBoundBroken)
		val mergedData = mergeBoundaryData(query, aggregate, slicedData, lowerBoundBroken, upperBoundBroken)
		mergedData
	}

	def sliceAggregate(query: Query, aggregate: CachedAggregate, lowerBoundBroken: Boolean, upperBoundBroken: Boolean): TreeMap[String,ReducedResult] = {
		var sliced = aggregate.aggregateMap

		if(lowerBoundBroken) {
			sliced = sliced.from(query.startPlusHour)
		}
		if(upperBoundBroken) {
			sliced = sliced.to(query.endHour.get)
		}

		sliced
	}

	def mergeBoundaryData(query: Query, aggregate: CachedAggregate, aggregateData: TreeMap[String,ReducedResult], lowerBoundBroken: Boolean, upperBoundBroken: Boolean): TreeMap[String,ReducedResult] = {
		var outOfBoundEvents: Iterable[Event] = Nil
		val startTS = query.startPlusHourDate.toInstant.getMillis
		val endTS = query.endHourDate.map(_.toInstant.getMillis)

		if(lowerBoundBroken && upperBoundBroken) {
			outOfBoundEvents = cache.limitEventsProcessed() { event: Event => 
				(event.ts > query.range.start && event.ts < startTS) &&
				(event.ts < query.range.end.get && event.ts > endTS.get)
			}
		} else if(lowerBoundBroken) {
			outOfBoundEvents = cache.limitEventsProcessed() { event: Event => (event.ts > query.range.start && event.ts < startTS) }
		} else if(upperBoundBroken) {
			outOfBoundEvents = cache.limitEventsProcessed() { event: Event => (event.ts < query.range.end.get && event.ts > endTS.get) }
		}

		val matchedEvents = QueryResolver.applyMatches(outOfBoundEvents, query.matches)
		val eventGroupings = QueryResolver.applyGroupings(matchedEvents, hourGrouping :: query.groupings)
		val boundaryAggregates = QueryResolver.applyReducerToEventGroupings(eventGroupings, aggregate.reducer)
		aggregateData ++ boundaryAggregates
	}

}

case class CachedAggregate(matchSet: Iterable[Match], groupSet: Iterable[Grouping], reducer: Reducer, var aggregateMap: TreeMap[String,ReducedResult])



