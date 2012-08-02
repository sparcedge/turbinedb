package com.sparcedge.turbine.blade.query.cache

import scala.collection.mutable
import scala.collection.immutable.TreeMap
import com.sparcedge.turbine.blade.query._
import akka.dispatch.{Await,Future,ExecutionContext}
import akka.util.duration._

class AggregateCache(cache: EventCache) {

	val aggregateCache = mutable.Map[String,Future[CachedAggregate]]()
	val aggregateGrouping = Grouping("duration", Some("minute"))

	// TODO: Clean up tuple mess
	def calculateQueryResults(query: Query)(implicit ec: ExecutionContext): TreeMap[String,Iterable[ReducedResult]] = {
		val timer = new Timer()

		timer.start()
		val aggregates = retrieveCachedAggregatesForQuery(query)
		timer.stop("Create/Retrieve Cached Aggregates")
		timer.start()
		val updatedAggregates = aggregates.map { case (property,aggregate) => (property,sliceAndMergeBoundaryData(query, aggregate)) }
		timer.stop("Slice/Merge Boundary Data")
		timer.start()
		val reducedAggregates = updatedAggregates.map { case (property,aggregate) => QueryResolver.removeHourGroupFlattendAndReduceAggregate(aggregate, property) }
		timer.stop("Flatten/Re-reduce Aggregates")
		timer.start()
		val flattened = QueryResolver.flattenAggregates(reducedAggregates)
		timer.stop("Flatten Grouped Aggregates")

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
		val aggregates = QueryResolver.matchGroupReduceEvents(cache.events, query.matches, aggregateGrouping :: query.groupings, reducer)
		new CachedAggregate (
			matchSet = query.matches,
			groupSet = query.groupings,
			reducer = reducer,
			aggregateMap = TreeMap(aggregates.toArray:_*)
		)
	}

	def sliceAndMergeBoundaryData(query: Query, aggregate: CachedAggregate): TreeMap[String,ReducedResult] = {
		val timer = new Timer()
		val lowerBoundBroken = query.range.start > cache.periodStart
		var upperBoundBroken = query.range.end != None && query.range.end.get < cache.periodEnd

		timer.start()
		val slicedData = sliceAggregate(query, aggregate, lowerBoundBroken, upperBoundBroken)
		timer.stop("Slice Aggregate", 1)

		timer.start()
		//mergeBoundaryData(query, aggregate, slicedData, lowerBoundBroken, upperBoundBroken)
		timer.stop("Merge Boundary Data", 1)

		slicedData
	}

	def sliceAggregate(query: Query, aggregate: CachedAggregate, lowerBoundBroken: Boolean, upperBoundBroken: Boolean): TreeMap[String,ReducedResult] = {
		val timer = new Timer()
		var sliced = aggregate.aggregateMap

		if(lowerBoundBroken) {
			timer.start()
			sliced = sliced.from(query.startPlusHour)
			timer.stop("Slice Lower Bound", 2)
		}
		if(upperBoundBroken) {
			timer.start()
			sliced = sliced.to(query.endHour.get)
			timer.stop("Slice Upper Bound", 2)
		}

		sliced
	}

	def mergeBoundaryData(query: Query, aggregate: CachedAggregate, aggregateData: TreeMap[String,ReducedResult], lowerBoundBroken: Boolean, upperBoundBroken: Boolean): TreeMap[String,ReducedResult] = {
		val timer = new Timer()
		var outOfBoundEvents: Iterable[Event] = Nil
		val startTS = query.startPlusHourDate.toInstant.getMillis
		val endTS = query.endHourDate.map(_.toInstant.getMillis)

		if(lowerBoundBroken && upperBoundBroken) {
			timer.start()
			outOfBoundEvents = cache.limitEventsProcessed() { event: Event => 
				(event.ts > query.range.start && event.ts < startTS) &&
				(event.ts < query.range.end.get && event.ts > endTS.get)
			}
			timer.stop("Limiting Lower and Upper Bound", 2)
		} else if(lowerBoundBroken) {
			timer.start()
			outOfBoundEvents = cache.limitEventsProcessed() { event: Event => (event.ts > query.range.start && event.ts < startTS) }
			timer.stop("Limiting Lower Only", 2)
		} else if(upperBoundBroken) {
			timer.start()
			outOfBoundEvents = cache.limitEventsProcessed() { event: Event => (event.ts < query.range.end.get && event.ts > endTS.get) }
			timer.stop("Limiting Upper Only", 2)
		}

		timer.start()
		val matchedEvents = QueryResolver.applyMatches(outOfBoundEvents, query.matches)
		timer.stop("Applying Matches To Out of Bounds", 2)
		timer.start()
		val eventGroupings = QueryResolver.applyGroupings(matchedEvents, aggregateGrouping :: query.groupings)
		timer.stop("Grouping Out of Bounds", 2)
		timer.start()
		val boundaryAggregates = QueryResolver.applyReducerToEventGroupings(eventGroupings, aggregate.reducer)
		timer.stop("Reducing Out of Bounds", 2)
		aggregateData ++ boundaryAggregates
	}

}

case class CachedAggregate(matchSet: Iterable[Match], groupSet: Iterable[Grouping], reducer: Reducer, var aggregateMap: TreeMap[String,ReducedResult])



