package com.sparcedge.turbine.blade.query.cache

import scala.collection.mutable
import scala.collection.immutable.TreeMap
import com.mongodb.casbah.query.Imports._
import com.mongodb.casbah.MongoCursor
import akka.dispatch.{Await,Future,ExecutionContext}
import akka.util.duration._
import com.sparcedge.turbine.blade.query._

object EventCache {
	def apply(eventCursor: MongoCursor, periodStart: Long, periodEnd: Long, includedFields: Set[String], blade: Blade): EventCache = {
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
		val update = EventCache.convertCursorToEventUpdate(eventCursor)
		applyEventUpdate(update)
	}

	def applyEventUpdate(eventUpdate: EventUpdate) {
		events ++= eventUpdate.events
		val updateTimestamp = eventUpdate.newestTimestamp
		if(updateTimestamp > newestTimestamp) {
			newestTimestamp = updateTimestamp
		}
	}

	def applyQuery(query: TurbineAnalyticsQuery)(implicit ec: ExecutionContext): String = {
		val hasReducers = query.query.reduce.map(_.reducerList.size > 0).getOrElse(false)
		var json = ""

		if(hasReducers) {
			val aggregateResults = caclulateAggregateResults(query.query)
			//val aggregateResults = calculateAggregateResultsFromCache(query.query)
			json = CustomJsonSerializer.serializeAggregateGroupMap(aggregateResults)
		} else {
			val timeLimitedEvents = limitEventsProcessed(query.query.range.start, query.query.range.end)
			var matchedEvents = QueryResolver.applyMatches(timeLimitedEvents, query.query.matches)
			matchedEvents = matchedEvents.take(1000)
			val eventGroups = QueryResolver.applyGroupings(matchedEvents, query.query.groupings)
			json = CustomJsonSerializer.serializeEventGroupMap(eventGroups)
		}

		json
	}

	def caclulateAggregateResults(query: Query): TreeMap[String,Iterable[ReducedResult]] = {
		val timeLimitedEvents = limitEventsProcessed(query.range.start, query.range.end)
		var matchedEvents = QueryResolver.applyMatches(timeLimitedEvents, query.matches)
		val eventGroups = QueryResolver.applyGroupings(matchedEvents, query.groupings)
		QueryResolver.applyReducersToEventGroupings(eventGroups, query.reduce.get.reducerList)
	}

	def calculateAggregateResultsFromCache(query: Query)(implicit ec: ExecutionContext): TreeMap[String,Iterable[ReducedResult]] = {
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



sealed abstract class DataElement(count: Int)

case class DataValue(value: Object, count: Int) extends DataElement(count)
case class DataGroup(value: List[DataElement], count: Int) extends DataElement(count)





















