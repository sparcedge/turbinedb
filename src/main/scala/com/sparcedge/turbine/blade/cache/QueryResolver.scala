package com.sparcedge.turbine.blade.cache

import scala.collection.mutable
import scala.collection.GenMap
import com.sparcedge.turbine.blade.query._
import com.sparcedge.turbine.blade.event.Event
import com.sparcedge.turbine.blade.util.{Timer,WrappedTreeMap}

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

	/* DISK BASED STREAMING*/

	def matchGroupReduceEventAndUpdateAggregateCalculations(event: Event, matches: Iterable[Match], groupings: Iterable[Grouping], aggregateCalculations: List[(Reducer,WrappedTreeMap[String,ReducedResult])]) {
		if(eventMatchesAllCriteria(event, matches)) {
			val grpStr = createGroupStringForEvent(event, groupings)
			aggregateCalculations foreach { case (reducer, resultMap) =>
				val streamingReducer = resultMap.getOrElseUpdate(grpStr, reducer.createReducedResult)
				streamingReducer(event)
			}
		}
	}

	def matchGroupReduceEventAndUpdateCachedAggregates(event: Event, cachedAggregates: Iterable[CachedAggregate]) {
		cachedAggregates.foreach { aggregate =>
			if(eventMatchesAllCriteria(event, aggregate.matchSet)) {
				val grpStr = createGroupStringForEvent(event, aggregate.groupSet)
				val streamingReducer = aggregate.aggregateMap.getOrElseUpdate(grpStr, aggregate.reducer.createReducedResult)
				streamingReducer(event)
			}
		}
	}

	/* END DISK BASED STREAMING*/

	def removeHourGroupFlattendAndReduceAggregate(aggregate: WrappedTreeMap[String,ReducedResult], output: String): WrappedTreeMap[String,ReducedResult] = {
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
		val reduced = new WrappedTreeMap[String,ReducedResult]()
		flattened.foreach { case (key, results) => 
			val result = Reduce.reReduce(results)
			result.output = Some(output)
			reduced(key) = result
		}
		timer.stop("Re-reduce Aggregates", 1)

		reduced
	}

	def flattenAggregates(aggregates: Iterable[WrappedTreeMap[String,ReducedResult]]): WrappedTreeMap[String,List[ReducedResult]] = {
		var flattened = new WrappedTreeMap[String,List[ReducedResult]]()
		aggregates foreach { aggregate =>
			aggregate foreach { case (key,value) =>
				val results = flattened.getOrElseUpdate(key, List[ReducedResult]())
				flattened(key) = (value :: results)
			}
		}
		flattened
	}
}