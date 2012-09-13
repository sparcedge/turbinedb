package com.sparcedge.turbine.blade.cache

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

	/* DISK BASED STREAMING*/

	def matchGroupReduceEventAndUpdateAggregateCalculations(event: Event, matches: Iterable[Match], groupings: Iterable[Grouping], aggregateCalculations: List[(Reducer,mutable.Map[String,ReducedResult])]) {
		if(eventMatchesAllCriteria(event, matches)) {
			val grpStr = createGroupStringForEvent(event, groupings)
			aggregateCalculations foreach { case (reducer, resultMap) =>
				val streamingReducer = resultMap.getOrElseUpdate(grpStr, reducer.createReducedResult)
				streamingReducer(event)
			}
		}
	}

	/* END DISK BASED STREAMING*/

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