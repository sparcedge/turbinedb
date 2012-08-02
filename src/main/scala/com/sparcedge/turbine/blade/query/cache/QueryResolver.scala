package com.sparcedge.turbine.blade.query.cache

import scala.collection.mutable
import scala.collection.GenMap
import scala.collection.immutable.TreeMap
import com.sparcedge.turbine.blade.query._

object QueryResolver {

	val GROUP_SEPARATOR = "✈"
	val GROUP_SEPARATOR_CHAR = '✈'

	/* STREAMING QUERY PROCESSING */
	def matchGroupReduceEvents(events: Iterable[Event], matches: Iterable[Match], groupings: Iterable[Grouping], reducers: Iterable[Reducer]): GenMap[String,Iterable[ReducedResult]] = {
		val matchedGroupedReducedEvents = mutable.Map[String,Iterable[ReducedResult]]()

		events foreach { event =>
			if(eventMatchesAllCriteria(event, matches)) {
				val grpStr = createGroupStringForEvent(event, groupings)
				val streamingReducers = matchedGroupedReducedEvents.getOrElseUpdate(grpStr, createStreamingReducers(reducers))
				updateStreamingReducersWithEvent(streamingReducers, event)
			}
		}
		matchedGroupedReducedEvents
	}

	def matchGroupReduceEvents(events: Iterable[Event], matches: Iterable[Match], groupings: Iterable[Grouping], reducer: Reducer): GenMap[String,ReducedResult] = {
		val matchedGroupedReducedEvents = mutable.Map[String,ReducedResult]()

		events foreach { event =>
			if(eventMatchesAllCriteria(event, matches)) {
				val grpStr = createGroupStringForEvent(event, groupings)
				val streamingReducer = matchedGroupedReducedEvents.getOrElseUpdate(grpStr, reducer.createReducedResult)
				streamingReducer(event)
			}
		}
		matchedGroupedReducedEvents
	}

	private def eventMatchesAllCriteria(event: Event, matches: Iterable[Match]): Boolean = {
		matches foreach { matcher =>
			if(!matcher(event)) {
				return false
			}
		}
		true
	}

	private def createGroupStringForEvent(event: Event, groupings: Iterable[Grouping]): String = {
		if(groupings.size > 0) {
			val builder = new StringBuilder
			builder.append(groupings.head.groupFunction(event))
			groupings.tail foreach { grouping =>
				builder.append(GROUP_SEPARATOR + grouping.groupFunction(event))
			}
			builder.toString
		} else {
			""
		}
	}

	private def createStreamingReducers(reducers: Iterable[Reducer]): Iterable[ReducedResult] = {
		val streamingReducers = mutable.ListBuffer[ReducedResult]()

		reducers foreach { reducer =>
			streamingReducers += reducer.createReducedResult
		}

		streamingReducers
	}

	private def updateStreamingReducersWithEvent(streamingReducers: Iterable[ReducedResult], event: Event) {
		streamingReducers foreach { sReducer =>
			sReducer(event)
		}
	}
	/* END STREAMING PROCESSING */


	/* NON-STREAMING QUERY PROCESSING */
	def applyGroupings(events: Iterable[Event], groupings: Iterable[Grouping]): TreeMap[String,Iterable[Event]] = {
		val eventGroupings = mutable.Map[String,List[Event]]()
		events.map(applyGroupingsToEvent(_, groupings)).foreach { case (groupStr, event) =>
			val groupedEvents = eventGroupings.getOrElseUpdate(groupStr, List[Event]())
			eventGroupings += (groupStr -> (event :: groupedEvents))
		}
		TreeMap(eventGroupings.toArray:_*)
	}

	def applyGroupingsToEvent(event: Event, groupings: Iterable[Grouping]): (String,Event) = {
		val groupStr = groupings.map(_.groupFunction(event)).mkString(GROUP_SEPARATOR)
		(groupStr,event)
	}

	def applyReducersToEventGroupings(eventGroupings: TreeMap[String, Iterable[Event]], reducers: Iterable[Reducer]): TreeMap[String, Iterable[ReducedResult]] = {
		eventGroupings.map { case (key, value) => (key, applyReducers(value, reducers)) }
	}

	def applyReducers(events: Iterable[Event], reducers: Iterable[Reducer]): Iterable[ReducedResult] = {
		reducers.map { reducer =>
			val result = reducer.reduceFunction(events)
			result.output = Some(reducer.propertyName)
			result
		}
	}
	
	def applyReducerToEventGroupings(eventGroupings: TreeMap[String, Iterable[Event]], reducer: Reducer): TreeMap[String, ReducedResult] = {
		eventGroupings.map { case (key, value) => (key, applyReducer(value, reducer)) }
	}

	def applyReducer(events: Iterable[Event], reducer: Reducer): ReducedResult = {
		reducer.reduceFunction(events)
	}

	def applyMatches(events: Iterable[Event], matchLst: Iterable[Match]): Iterable[Event] = {
		matchLst match {
			case Nil =>
				events
			case matches =>
				events filter { event =>
					matches forall { _(event) }
				}
		}
	}
	/* END NON-STREAMING QUERY PROCESSING */


	def removeHourGroupFlattendAndReduceAggregate(aggregate: TreeMap[String,ReducedResult], output: String): TreeMap[String,ReducedResult] = {
		val timer = new Timer()
		var flattened = mutable.Map[String,List[ReducedResult]]()
		timer.start()
		aggregate foreach { case (key,value) =>
			val newKey = key.dropWhile(_ != GROUP_SEPARATOR_CHAR).stripPrefix(GROUP_SEPARATOR)
			val results = flattened.getOrElseUpdate(newKey, List[ReducedResult]())
			flattened +=  (newKey -> (value :: results))
		}
		timer.stop("Flatten Aggregates", 1)
		timer.start()
		val reduced = flattened.mapValues { results => 
			val result = Reduce.reReduce(results)
			result.output = Some(output)
			result
		}
		timer.stop("Re-reduce Aggregates", 1)

		TreeMap(reduced.toArray:_*)
	}

	def flattenAggregates(aggregates: List[TreeMap[String,ReducedResult]]): TreeMap[String,Iterable[ReducedResult]] = {
		var flattened = mutable.Map[String,List[ReducedResult]]()
		aggregates foreach { aggregate =>
			aggregate foreach { case (key,value) =>
				val results = flattened.getOrElseUpdate(key, List[ReducedResult]())
				flattened += (key -> (value :: results))
			}
		}
		TreeMap(flattened.toArray:_*)
	}
}