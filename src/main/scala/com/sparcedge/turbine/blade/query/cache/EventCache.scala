package com.sparcedge.turbine.blade.query.cache

import scala.collection.mutable
import scala.collection.immutable.TreeMap
import scala.collection.GenMap
import com.mongodb.casbah.query.Imports._
import com.mongodb.casbah.MongoCursor
import akka.dispatch.{Await,Future,ExecutionContext}
import akka.util.duration._
import com.sparcedge.turbine.blade.query._

object EventCache {
	def apply(eventCursor: MongoCursor, periodStart: Long, periodEnd: Long, includedFields: Set[String], blade: Blade): EventCache = {
		val timer = new Timer
		val events = mutable.ListBuffer[Event]()
		val partitionManager = PartitionManager()
		var newestTimestamp = 0L

		timer.start()
		eventCursor foreach { event =>
			events += Event(event, partitionManager)
			val its: Long = event("its") match { 
				case x: java.lang.Long => x
				case x: java.lang.Double => x.toLong 
				case _ => 0L
			}
			if(its > newestTimestamp) {
				newestTimestamp = its
			}
		}
		timer.stop("Populated Cache (" + blade.period + ")")
		new EventCache(events, periodStart, periodEnd, includedFields, newestTimestamp, blade)
	}

	def convertCursorToEventUpdate(eventCursor: MongoCursor): EventUpdate = {
		val events = mutable.ListBuffer[Event]()
		val partitionManager = PartitionManager()
		var newestTimestamp = 0L
		eventCursor foreach { event =>
			events += Event(event, partitionManager)
			val its: Long = event("its") match { 
				case x: java.lang.Long => x
				case x: java.lang.Double => x.toLong 
				case _ => 0L
			}
			if(its > newestTimestamp) {
				newestTimestamp = its
			}
		}
		EventUpdate(events, newestTimestamp)
	}
}

class EventCache(val events: mutable.ListBuffer[Event], val periodStart: Long, val periodEnd: Long, val includedFields: Set[String], var newestTimestamp: Long, val blade: Blade) {

	val aggregateCache = new AggregateCache(this)

	def addEventsToCache(eventCursor: MongoCursor) {
		val timer = new Timer()
		timer.start()
		val update = EventCache.convertCursorToEventUpdate(eventCursor)
		applyEventUpdate(update)
		timer.stop("Updated Event Cache")
	}

	def applyEventUpdate(eventUpdate: EventUpdate) {
		events ++= eventUpdate.events
		val updateTimestamp = eventUpdate.newestTimestamp
		if(updateTimestamp > newestTimestamp) {
			newestTimestamp = updateTimestamp
		}
		aggregateCache.updateAggregateCache(eventUpdate.events)
	}

	def applyQuery(query: TurbineAnalyticsQuery)(implicit ec: ExecutionContext): String = {
		val timer = new Timer()
		val hasReducers = query.query.reduce.map(_.reducerList.size > 0).getOrElse(false)
		var json = ""

		if(hasReducers) {
			timer.start()
			val aggregateResults = calculateAggregateResultsFromCache(query)
			var endTime = System.currentTimeMillis
			timer.stop("[" + query.qid + "] Query Processing")
			timer.start()
			json = CustomJsonSerializer.serializeAggregateGroupMap(aggregateResults)
			timer.stop("[" + query.qid + "] Serialize Results")
		} else {
			val timeLimitedEvents = limitEventsProcessed(query.query.range.start, query.query.range.end)
			var matchedEvents = QueryResolver.applyMatches(timeLimitedEvents, query.query.matches)
			matchedEvents = matchedEvents.take(1000)
			val eventGroups = QueryResolver.applyGroupings(matchedEvents, query.query.groupings)
			json = CustomJsonSerializer.serializeEventGroupMap(eventGroups)
		}

		json
	}

	def calculateAggregateResultsStreaming(query: Query): GenMap[String,Iterable[ReducedResult]] = {
		val timer = new Timer()
		timer.start()
		val timeLimitedEvents = limitEventsProcessed(query.range.start, query.range.end)
		timer.stop("Limit Events Processed By Time")
		timer.start()
		val results = QueryResolver.matchGroupReduceEvents(timeLimitedEvents, query.matches, query.groupings, query.reduce.get.reducerList)
		timer.stop("Streaming Match Group Reduce Events")
		results
	}

	def calculateAggregateResultsFromCache(query: TurbineAnalyticsQuery)(implicit ec: ExecutionContext): TreeMap[String,Iterable[ReducedResult]] = {
		aggregateCache.calculateQueryResults(query)
	}

	def limitEventsProcessed(start: Long, end: Option[Long]): Iterable[Event] = {
		if(start > periodStart || (end != None && end.get < periodEnd)) {
			events.filter(event => event.ts > start && (end == None || event.ts < end.get))
		} else {
			events
		}
	}

	def limitEventsProcessed()(fun: (Event) => Boolean): Iterable[Event] = {
		events.filter(fun)
	}

	def includesAllFields(fields: Set[String]): Boolean = {
		fields.subsetOf(includedFields)
	}

	def getNotIncludedFields(fields: Set[String]): Set[String] = {
		includedFields.diff(fields)
	}
}

case class EventUpdate (
	events: mutable.ListBuffer[Event],
	newestTimestamp: Long
)





















